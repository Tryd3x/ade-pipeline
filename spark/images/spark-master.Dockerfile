FROM spark-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends openssh-server sudo && \
    mkdir -p /var/run/sshd && \
    # Create spark-user for SSH access
    useradd -rm -d /home/spark-user -s /bin/bash -g root -G sudo -u 1000 spark-user && \
    echo "spark-user:sparkpassword" | chpasswd && \
    # Setup SSH config
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    # Generate SSH keys inside the container
    mkdir -p /home/spark-user/.ssh && \
    ssh-keygen -t rsa -b 4096 -f /home/spark-user/.ssh/id_rsa -N "" && \
    cp /home/spark-user/.ssh/id_rsa.pub /home/spark-user/.ssh/authorized_keys && \
    chown -R spark-user:root /home/spark-user/.ssh && \
    chmod 600 /home/spark-user/.ssh/authorized_keys && \
    # Clean up
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*