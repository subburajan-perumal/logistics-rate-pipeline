# EKS for one 3-hour window (docs/PLAN.md §9.4, D-25):
#   VPC with two public subnets and NO NAT gateway, an EKS cluster on a
#   standard-support minor, one t3.small node, Pod Identity for S3.
# Apply → deploy the chart → run → `terraform destroy` the same day.
#
#   terraform init && terraform apply -var bucket=<your-bucket>
#   aws eks update-kubeconfig --name rates --region us-east-1
#   ...
#   terraform destroy -var bucket=<your-bucket>

terraform {
  required_version = ">= 1.9"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 6.0" }
  }
}

provider "aws" {
  region = var.region
  default_tags { tags = { project = "logistics-rate-pipeline" } }
}

variable "region" { default = "us-east-1" }
variable "cluster_name" { default = "rates" }
variable "kubernetes_version" {
  description = "Pick a minor in STANDARD support on the day (extended support is 6x the price)."
  default     = "1.36"
}
variable "bucket" {
  description = "Existing S3 bucket the ingestd Pod Identity role may write to."
  type        = string
}
variable "instance_type" { default = "t3.small" }

data "aws_availability_zones" "azs" { state = "available" }

# ---------------- VPC: public subnets only (no NAT bill) ----------------
resource "aws_vpc" "this" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
  tags                 = { Name = "${var.cluster_name}-vpc" }
}

resource "aws_internet_gateway" "igw" { vpc_id = aws_vpc.this.id }

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.this.id
  cidr_block              = cidrsubnet(aws_vpc.this.cidr_block, 8, count.index)
  availability_zone       = data.aws_availability_zones.azs.names[count.index]
  map_public_ip_on_launch = true
  tags = {
    Name                                        = "${var.cluster_name}-public-${count.index}"
    "kubernetes.io/role/elb"                    = "1"
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.igw.id
  }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

# ---------------- IAM for the control plane and nodes ----------------
data "aws_iam_policy_document" "eks_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "cluster" {
  name               = "${var.cluster_name}-eks-cluster"
  assume_role_policy = data.aws_iam_policy_document.eks_assume.json
}

resource "aws_iam_role_policy_attachment" "cluster" {
  role       = aws_iam_role.cluster.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
}

data "aws_iam_policy_document" "node_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "${var.cluster_name}-eks-node"
  assume_role_policy = data.aws_iam_policy_document.node_assume.json
}

resource "aws_iam_role_policy_attachment" "node" {
  for_each = toset([
    "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy",
    "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy",
    "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly",
  ])
  role       = aws_iam_role.node.name
  policy_arn = each.value
}

# ---------------- Cluster + node group + addons ----------------
resource "aws_eks_cluster" "this" {
  name     = var.cluster_name
  version  = var.kubernetes_version
  role_arn = aws_iam_role.cluster.arn

  vpc_config {
    subnet_ids              = aws_subnet.public[*].id
    endpoint_public_access  = true
    endpoint_private_access = false
  }

  access_config {
    authentication_mode                         = "API_AND_CONFIG_MAP"
    bootstrap_cluster_creator_admin_permissions = true
  }

  depends_on = [aws_iam_role_policy_attachment.cluster]
}

resource "aws_eks_node_group" "default" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = "default"
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = aws_subnet.public[*].id
  instance_types  = [var.instance_type]
  capacity_type   = "ON_DEMAND" # spot interruptions would confound the mid-run test

  scaling_config {
    desired_size = 1
    min_size     = 1
    max_size     = 1
  }

  depends_on = [aws_iam_role_policy_attachment.node]
}

resource "aws_eks_addon" "addons" {
  for_each     = toset(["vpc-cni", "coredns", "kube-proxy", "eks-pod-identity-agent"])
  cluster_name = aws_eks_cluster.this.name
  addon_name   = each.value
  depends_on   = [aws_eks_node_group.default]
}

# ---------------- Pod Identity: ingestd → S3, no keys in the cluster ----------------
data "aws_iam_policy_document" "pod_identity_assume" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "ingestd" {
  name               = "${var.cluster_name}-ingestd"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_assume.json
}

data "aws_iam_policy_document" "ingestd_s3" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = ["arn:aws:s3:::${var.bucket}"]
  }
  statement {
    actions   = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
    resources = ["arn:aws:s3:::${var.bucket}/*"]
  }
}

resource "aws_iam_role_policy" "ingestd_s3" {
  role   = aws_iam_role.ingestd.id
  policy = data.aws_iam_policy_document.ingestd_s3.json
}

resource "aws_eks_pod_identity_association" "ingestd" {
  cluster_name    = aws_eks_cluster.this.name
  namespace       = "rates"
  service_account = "rates-rate-pipeline-ingestd" # chart default: <release>-rate-pipeline-ingestd
  role_arn        = aws_iam_role.ingestd.arn
  depends_on      = [aws_eks_addon.addons]
}

output "cluster_name" { value = aws_eks_cluster.this.name }
output "kubeconfig_command" {
  value = "aws eks update-kubeconfig --name ${aws_eks_cluster.this.name} --region ${var.region}"
}
output "ingestd_role_arn" { value = aws_iam_role.ingestd.arn }
