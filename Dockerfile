FROM sirisurab/tp-app-pkgs AS app
ARG SSH_PRIVATE_KEY
ARG SSH_PUB_KEY
ARG SSH_KEY_PASSPHRASE
RUN rm -Rf /app && mkdir /app && \
mkdir -p $HOME/.ssh && \
echo "${SSH_PRIVATE_KEY}" > $HOME/.ssh/id_rsa && \
echo "${SSH_PUB_KEY}" > $HOME/.ssh/id_rsa.pub && \
chmod -R 400 $HOME/.ssh/ && \
ssh-keyscan github.com > $HOME/.ssh/known_hosts && \
eval $(ssh-agent -s) && \
printf "${SSH_KEY_PASSPHRASE}\n" | ssh-add $HOME/.ssh/id_rsa && \
git clone -v "ssh://git@github.com/sirisurab/transpred.git" /app

# Set the working directory to /app
WORKDIR /app
ENTRYPOINT ["python"]