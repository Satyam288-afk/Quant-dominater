variable "region" {
  description = "AWS region to deploy the benchmark platform into."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev/staging/prod)."
  type        = string
  default     = "dev"
}

variable "cluster_name" {
  description = "EKS cluster name."
  type        = string
  default     = "iicpc-benchmark"
}

variable "cluster_version" {
  description = "Kubernetes control plane version."
  type        = string
  default     = "1.30"
}

variable "vpc_cidr" {
  description = "CIDR block for the platform VPC."
  type        = string
  default     = "10.42.0.0/16"
}

variable "cluster_public_access_cidrs" {
  description = <<-EOT
    CIDRs allowed to reach the EKS public API endpoint. Defaults to a
    NON-open placeholder (TEST-NET documentation block) so the endpoint is
    never inadvertently exposed to 0.0.0.0/0. PIN THIS to the operator /
    CI egress CIDR(s) before deploying, or set cluster_endpoint_public_access
    = false and reach the API over the private endpoint / a bastion.
  EOT
  type        = list(string)
  default     = ["192.0.2.0/24"] # RFC 5737 TEST-NET-1 placeholder — override me
}

variable "platform_instance_types" {
  description = "Instance types for the general platform node group (control + data plane, bot fleet, ingester)."
  type        = list(string)
  default     = ["m6i.xlarge"]
}

variable "platform_min_size" {
  type    = number
  default = 2
}

variable "platform_max_size" {
  description = "Upper bound for horizontal scale-out of the platform node group."
  type        = number
  default     = 10
}

variable "sandbox_instance_types" {
  description = "Instance types for the isolated contestant-sandbox node group. Compute-optimized for fair, pinned CPU."
  type        = list(string)
  default     = ["c6i.2xlarge"]
}

variable "sandbox_min_size" {
  type    = number
  default = 1
}

variable "sandbox_max_size" {
  type    = number
  default = 8
}

variable "service_images" {
  description = "Service names that get a dedicated ECR repository."
  type        = list(string)
  default = [
    "submission-api",
    "sandbox-runner",
    "orchestrator",
    "score-engine",
    "leaderboard-api",
    "telemetry-ingester",
    "bot-fleet",
  ]
}
