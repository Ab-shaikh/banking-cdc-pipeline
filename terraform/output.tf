output "instance_public_ip" {
  description = "Public IP address of the EC2 instance"
  value       = aws_instance.cdc_lab_ec2.public_ip
}

output "instance_public_dns" {
  description = "Public DNS of the EC2 instance"
  value       = aws_instance.cdc_lab_ec2.public_dns
}

output "ssh_command" {
  description = "SSH command to connect"
  value       = "ssh -i \"${var.key_pair_name}.pem\" ubuntu@${aws_instance.cdc_lab_ec2.public_ip}"
}