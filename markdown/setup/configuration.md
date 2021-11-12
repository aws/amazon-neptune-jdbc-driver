# Using Gremlin with Amazon Neptune JDBC Driver

To connect to Amazon Neptune using the JDBC driver, the Neptune instance must be available through an SSH tunnel, load balancer, or the JDBC driver must be deployed in an EC2 instance. The SSH tunnel can be setup following the instructions below.

### Using an SSH Tunnel to Connect to Amazon Neptune

Amazon Neptune clusters are deployed within an Amazon Virtual Private Cloud (Amazon VPC).
They can be accessed directly by Amazon EC2 instances or other AWS services that are deployed in the same Amazon VPC. Additionally, Amazon Neptune can be accessed by EC2 instances or other AWS services in different VPCs in the same AWS Region or other Regions via VPC peering.

However, suppose that your use case requires that you (or your application) access your Amazon Neptune resources from outside the cluster's VPC. This will be the case for most users not running their application on a VM in the same VPC as the Neptune cluster. When connecting from outside the VPC, you can use SSH tunneling (also known as  _port forwarding_) to access your Amazon Neptune resources.

To create an SSH tunnel, you need an Amazon EC2 instance running in the same Amazon VPC as your Amazon Neptune cluster. You can either use an existing EC2 instance in the same VPC as your cluster or create one.

You can set up an SSH tunnel to the Amazon Neptune cluster `sample-cluster.node.us-east-1.neptune.amazonaws.com` by running the following command on your local computer. The `-L` flag is used for forwarding a local port.

Note: The username of the ec2 connection depends on the type of your ec2 instance. In the below example, we are using Ubuntu.
```
ssh -i "ec2Access.pem" -L 8182:sample-cluster.node.us-east-1.docdb.amazonaws.com:8182 ubuntu@ec2-34-229-221-164.compute-1.amazonaws.com -N 
```

This is a prerequisite for connecting to any BI tool running on a client outside your VPC.

#### Important: You must also add the SSH tunnel lookup detailed below.

### Adding a SSH Tunnel Lookup Connection to Amazon Neptune

To add to hosts, add the cluster name to your host file (`/etc/hosts` on Mac or `C:\Windows\System32\drivers\etc\hosts` on Windows).
Add the following line to the list of hosts lookup:
`127.0.0.1        <endpoint>`

From our sample above, we would use:
`127.0.0.1        sample-cluster.node.us-east-1.docdb.amazonaws.com`