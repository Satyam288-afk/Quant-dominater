output "region" {
  description = "AWS region."
  value       = var.region
}

output "cluster_name" {
  description = "EKS cluster name."
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS API server endpoint."
  value       = module.eks.cluster_endpoint
}

output "configure_kubectl" {
  description = "Command to point kubectl at the new cluster."
  value       = "aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}"
}

output "vpc_id" {
  description = "Platform VPC id."
  value       = module.vpc.vpc_id
}

output "ecr_repository_urls" {
  description = "ECR repository URL per service (push images here, then retag the k8s manifests)."
  value       = { for name, repo in aws_ecr_repository.service : name => repo.repository_url }
}
