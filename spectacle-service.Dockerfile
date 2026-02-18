FROM node:20-slim

ARG USER=openhouse
ARG USER_ID=1000
ARG GROUP_ID=1000
ENV APP_NAME=spectacle
ENV USER_HOME=/home/$USER

# Remove existing node user/group if they conflict, then create openhouse user
RUN if getent passwd $USER_ID > /dev/null 2>&1; then \
        userdel -f $(getent passwd $USER_ID | cut -d: -f1); \
    fi && \
    if getent group $GROUP_ID > /dev/null 2>&1; then \
        groupdel $(getent group $GROUP_ID | cut -d: -f1) 2>/dev/null || true; \
    fi && \
    groupadd -g $GROUP_ID $USER && \
    useradd -l -d $USER_HOME -m $USER -u $USER_ID -g $GROUP_ID

WORKDIR $USER_HOME/app

# Copy package files
COPY services/spectacle/package*.json ./

# Install dependencies
RUN npm ci --only=production

# Copy application files
COPY services/spectacle/ ./

# Build the Next.js application
RUN npm run build

# Ensure that everything in $USER_HOME is owned by openhouse user
RUN chown -R openhouse:openhouse $USER_HOME

USER $USER

EXPOSE 3000

ENV NODE_ENV=production
ENV PORT=3000

CMD ["npm", "start"]
