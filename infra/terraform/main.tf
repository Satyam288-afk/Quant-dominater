data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 3)

  # Tags that let the AWS Load Balancer Controller and the cluster autoscaler
  # discover subnets / the cluster.
  cluster_tags = {
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

#############################################
# Networking
#############################################
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.8"

  name = "${var.cluster_name}-vpc"
  cidr = var.vpc_cidr
  azs  = local.azs

  private_subnets = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 4, k)]
  public_subnets  = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 4, k + 8)]

  enable_nat_gateway   = true
  single_nat_gateway   = true
  enable_dns_hostnames = true

  public_subnet_tags = merge(local.cluster_tags, {
    "kubernetes.io/role/elb" = "1"
  })
  private_subnet_tags = merge(local.cluster_tags, {
    "kubernetes.io/role/internal-elb" = "1"
  })
}

#############################################
# EKS cluster + node groups
#############################################
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.24"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version

  cluster_endpoint_public_access           = true
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  cluster_addons = {
    coredns                = {}
    kube-proxy             = {}
    vpc-cni                = {}
    eks-pod-identity-agent = {}
  }

  eks_managed_node_groups = {
    # General platform pool: control plane services, data plane, bot fleet,
    # ingester. Scales horizontally for load.
    platform = {
      instance_types = var.platform_instance_types
      min_size       = var.platform_min_size
      max_size       = var.platform_max_size
      desired_size   = var.platform_min_size
      labels         = { pool = "platform" }
    }

    # Dedicated, tainted pool for untrusted contestant sandboxes. The taint
    # keeps platform workloads off these nodes; the static CPU manager policy
    # enables exclusive core pinning for fair latency measurement.
    sandbox = {
      instance_types = var.sandbox_instance_types
      min_size       = var.sandbox_min_size
      max_size       = var.sandbox_max_size
      desired_size   = var.sandbox_min_size
      labels         = { pool = "sandbox" }
      taints = {
        sandbox = {
          key    = "iicpc.dev/sandbox"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }
      # AL2 bootstrap: turn on static CPU pinning + reserve resources for the
      # kubelet so Guaranteed-QoS contestant pods get exclusive cores.
      bootstrap_extra_args = "--kubelet-extra-args '--cpu-manager-policy=static --kube-reserved cpu=300m,memory=512Mi --system-reserved cpu=200m,memory=512Mi'"
    }
  }
}

#############################################
# Container registry (one repo per service)
#############################################
resource "aws_ecr_repository" "service" {
  for_each = toset(var.service_images)

  name                 = "iicpc/${each.value}"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "service" {
  for_each   = aws_ecr_repository.service
  repository = each.value.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 20 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 20
      }
      action = { type = "expire" }
    }]
  })
}
