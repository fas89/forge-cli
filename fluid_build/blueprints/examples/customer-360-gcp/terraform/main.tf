terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta" 
      version = "~> 5.0"
    }
  }
}

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "bigquery_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

# Local values
locals {
  datasets = {
    raw     = "customer360_raw_${var.environment}"
    staging = "customer360_staging_${var.environment}"
    marts   = "customer360_marts_${var.environment}"
    ml      = "customer360_ml_${var.environment}"
  }
  
  common_labels = {
    environment = var.environment
    project     = "customer-360"
    managed_by  = "terraform"
    team        = "data-platform"
  }
}

# Enable required APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "bigqueryml.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "cloudfunctions.googleapis.com",
    "cloudscheduler.googleapis.com",
    "secretmanager.googleapis.com"
  ])
  
  project = var.project_id
  service = each.value
  
  disable_on_destroy = false
}

# BigQuery Datasets
resource "google_bigquery_dataset" "datasets" {
  for_each = local.datasets
  
  dataset_id                  = each.value
  friendly_name              = title(replace(each.key, "_", " "))
  description                = "Customer 360 ${each.key} dataset for ${var.environment}"
  location                   = var.bigquery_location
  default_table_expiration_ms = each.key == "raw" ? 2592000000 : null # 30 days for raw
  
  labels = local.common_labels

  # Access controls
  dynamic "access" {
    for_each = each.key == "raw" ? [1] : []
    content {
      role          = "OWNER"
      user_by_email = data.google_client_openid_userinfo.me.email
    }
  }

  dynamic "access" {
    for_each = each.key != "raw" ? [1] : []
    content {
      role          = "READER"
      special_group = "projectReaders"
    }
  }

  dynamic "access" {
    for_each = each.key != "raw" ? [1] : []
    content {
      role          = "WRITER"
      special_group = "projectWriters"
    }
  }

  depends_on = [google_project_service.apis]
}

# Get current user info
data "google_client_openid_userinfo" "me" {}

# Cloud Storage Buckets
resource "google_storage_bucket" "customer_events_raw" {
  name          = "${var.project_id}-customer-events-raw-${var.environment}"
  location      = var.region
  force_destroy = var.environment != "prod"
  
  labels = local.common_labels

  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
  
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

resource "google_storage_bucket" "dbt_artifacts" {
  name          = "${var.project_id}-dbt-artifacts-${var.environment}"
  location      = var.region
  force_destroy = var.environment != "prod"
  
  labels = local.common_labels

  uniform_bucket_level_access = true
  
  versioning {
    enabled = true
  }
  
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }
}

# Pub/Sub Topic for real-time events
resource "google_pubsub_topic" "customer_events" {
  name = "customer-events-${var.environment}"
  
  labels = local.common_labels
  
  schema_settings {
    schema   = google_pubsub_schema.customer_events.id
    encoding = "JSON"
  }

  depends_on = [google_project_service.apis]
}

# Pub/Sub Schema for events
resource "google_pubsub_schema" "customer_events" {
  name       = "customer-events-schema-${var.environment}"
  type       = "AVRO"
  definition = jsonencode({
    type = "record"
    name = "CustomerEvent"
    fields = [
      {
        name = "event_id"
        type = "string"
      },
      {
        name = "customer_id"
        type = "string"
      },
      {
        name = "session_id"
        type = "string"
      },
      {
        name = "event_timestamp"
        type = {
          type        = "long"
          logicalType = "timestamp-millis"
        }
      },
      {
        name = "event_type"
        type = {
          type = "enum"
          name = "EventType"
          symbols = [
            "PAGE_VIEW",
            "PRODUCT_VIEW", 
            "ADD_TO_CART",
            "PURCHASE",
            "SEARCH",
            "LOGIN",
            "LOGOUT",
            "SUPPORT_CONTACT"
          ]
        }
      },
      {
        name = "event_properties"
        type = ["null", "string"]
        default = null
      },
      {
        name = "user_agent"
        type = ["null", "string"]
        default = null
      },
      {
        name = "ip_address_hash"
        type = ["null", "string"]
        default = null
      },
      {
        name = "referrer"
        type = ["null", "string"]
        default = null
      },
      {
        name = "utm_parameters"
        type = ["null", "string"]
        default = null
      },
      {
        name = "device_type"
        type = ["null", "string"]
        default = null
      },
      {
        name = "channel"
        type = ["null", "string"]
        default = null
      },
      {
        name = "revenue_amount"
        type = ["null", "double"]
        default = null
      }
    ]
  })

  depends_on = [google_project_service.apis]
}

# Pub/Sub Subscription for Dataflow
resource "google_pubsub_subscription" "customer_events_dataflow" {
  name  = "customer-events-dataflow-${var.environment}"
  topic = google_pubsub_topic.customer_events.name

  labels = local.common_labels

  # Dataflow needs acknowledgment deadline
  ack_deadline_seconds = 600

  # Dead letter policy for failed messages
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.customer_events_dlq.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# Dead letter queue topic
resource "google_pubsub_topic" "customer_events_dlq" {
  name = "customer-events-dlq-${var.environment}"
  
  labels = local.common_labels
}

# Service Account for Dataflow
resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-customer360-${var.environment}"
  display_name = "Customer 360 Dataflow Service Account"
  description  = "Service account for Customer 360 Dataflow jobs"
}

# IAM bindings for Dataflow service account
resource "google_project_iam_member" "dataflow_permissions" {
  for_each = toset([
    "roles/dataflow.worker",
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/pubsub.subscriber"
  ])
  
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# Service Account for dbt Cloud
resource "google_service_account" "dbt_sa" {
  account_id   = "dbt-customer360-${var.environment}"
  display_name = "Customer 360 dbt Service Account"
  description  = "Service account for dbt transformations"
}

# IAM bindings for dbt service account
resource "google_project_iam_member" "dbt_permissions" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectViewer"
  ])
  
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dbt_sa.email}"
}

# Service Account Key for dbt (consider using Workload Identity instead for production)
resource "google_service_account_key" "dbt_key" {
  service_account_id = google_service_account.dbt_sa.name
  public_key_type    = "TYPE_X509_PEM_FILE"
}

# Store service account key in Secret Manager
resource "google_secret_manager_secret" "dbt_service_account_key" {
  secret_id = "dbt-service-account-key-${var.environment}"
  
  labels = local.common_labels

  replication {
    automatic = true
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "dbt_service_account_key" {
  secret      = google_secret_manager_secret.dbt_service_account_key.id
  secret_data = base64decode(google_service_account_key.dbt_key.private_key)
}

# Cloud Scheduler for dbt jobs
resource "google_cloud_scheduler_job" "dbt_daily_run" {
  name             = "dbt-customer360-daily-${var.environment}"
  description      = "Daily dbt run for Customer 360"
  schedule         = "0 2 * * *" # 2 AM daily
  time_zone        = "UTC"
  attempt_deadline = "3600s"

  http_target {
    http_method = "POST"
    uri         = "https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    
    headers = {
      "Content-Type"  = "application/json"
      "Authorization" = "Token ${var.dbt_cloud_token}"
    }
    
    body = base64encode(jsonencode({
      cause = "Scheduled via Cloud Scheduler"
    }))
  }

  depends_on = [google_project_service.apis]
}

# Outputs
output "bigquery_datasets" {
  description = "Created BigQuery datasets"
  value       = { for k, v in google_bigquery_dataset.datasets : k => v.dataset_id }
}

output "storage_buckets" {
  description = "Created storage buckets"
  value = {
    raw_events    = google_storage_bucket.customer_events_raw.name
    dbt_artifacts = google_storage_bucket.dbt_artifacts.name
  }
}

output "pubsub_topic" {
  description = "Pub/Sub topic for customer events"
  value       = google_pubsub_topic.customer_events.name
}

output "service_accounts" {
  description = "Created service accounts"
  value = {
    dataflow = google_service_account.dataflow_sa.email
    dbt      = google_service_account.dbt_sa.email
  }
}

output "secret_manager_secrets" {
  description = "Secret Manager secrets"
  value = {
    dbt_service_account_key = google_secret_manager_secret.dbt_service_account_key.secret_id
  }
}