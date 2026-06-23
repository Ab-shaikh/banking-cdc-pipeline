variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "ap-south-1"
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "m7i-flex.large"
}

variable "key_pair_name" {
  description = "Name of existing EC2 key pair for SSH access"
  type        = string
  default     = "raisa"
}

variable "my_ip" {
  description = "Your IP address for SSH access (CIDR format)"
  type        = string
}

variable "iam_role_name" {
  description = "Existing IAM role name to attach to EC2 (S3 access)"
  type        = string
  default     = "s3_access_ec2"
}