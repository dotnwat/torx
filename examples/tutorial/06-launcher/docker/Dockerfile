# The tutorial's node image: an sshd the torx ssh backend can log in to, a
# POSIX sh (busybox, whose setsid torx uses to give each streamed command a
# process group of its own), and kvd on PATH.
#
# The build context is a run directory the launcher prepared: bin/kvd is the
# server cross-compiled for this platform, and keys/ holds the host key the
# node presents and the client key it accepts logins from.
FROM alpine:3.22

# openssh brings sshd and the sftp subsystem the backend moves files with.
# adduser locks the new account (a "!" password), and sshd refuses a locked
# account even for key authentication; "*" is unlocked with no password.
# /var/tmp/torx is the node's scratch root, as the manifest names it.
RUN apk add --no-cache openssh \
 && adduser -D torx \
 && sed -i 's/^torx:!:/torx:*:/' /etc/shadow \
 && install -d -o torx -g torx -m 700 /home/torx/.ssh \
 && install -d -o torx -g torx -m 755 /var/tmp/torx

COPY --chmod=755 bin/kvd /usr/local/bin/kvd
COPY docker/sshd_config /etc/ssh/sshd_config
COPY --chmod=600 keys/host_key /etc/ssh/ssh_host_ed25519_key
COPY --chown=torx:torx --chmod=644 keys/authorized_keys /home/torx/.ssh/authorized_keys

CMD ["/usr/sbin/sshd", "-D", "-e"]
