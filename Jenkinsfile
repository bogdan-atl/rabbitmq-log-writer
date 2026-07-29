pipeline {
    agent {
        node {
            label 'niva-local'
            retries 0
        }
    }

    environment {
        BINARY_NAME  = 'udp-logger'
        SERVICE_NAME = 'udp-logger-client.service'
        TARGET_DIR   = '/root/rabbit-log-writer'
        REDIS_CONTAINER = 'udp-logger-ci-redis'
        REDIS_PORT = '6379'
        REDIS_DATA_DIR = '/var/redis'
        REMOVE_REDIS_ON_FINISH = 'false'
    }

    stages {
        stage('Clean') {
            steps {
                sh '''
                    rm -f ${BINARY_NAME}
                '''
            }
        }

        stage('Build') {
            steps {
                sh '''
                    go mod tidy
                    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ${BINARY_NAME} ./cmd/udp-logger
                '''
                sh 'ls -la ${BINARY_NAME}'
            }
        }

        stage('Start Redis') {
            steps {
                sh '''
                    sudo mkdir -p ${REDIS_DATA_DIR}
                    docker rm -f ${REDIS_CONTAINER} >/dev/null 2>&1 || true
                    docker run -d --name ${REDIS_CONTAINER} \
                        --restart unless-stopped \
                        -p ${REDIS_PORT}:6379 \
                        -v ${REDIS_DATA_DIR}:/data \
                        redis:7-alpine --appendonly yes
                    sleep 3
                    docker exec ${REDIS_CONTAINER} redis-cli ping
                '''
            }
        }

        stage('Archive Artifact') {
            steps {
                archiveArtifacts artifacts: "${BINARY_NAME}", fingerprint: true
            }
        }

        stage('Deploy') {
            steps {
                sh '''
                    sudo systemctl stop ${SERVICE_NAME} || true
                    sudo mkdir -p ${TARGET_DIR}
                    sudo cp ${BINARY_NAME} ${TARGET_DIR}/${BINARY_NAME}
                    sudo chown root:root ${TARGET_DIR}/${BINARY_NAME}
                    sudo chmod +x ${TARGET_DIR}/${BINARY_NAME}
                '''
            }
        }

        stage('Restart Service') {
            steps {
                sh '''
                    sudo systemctl daemon-reload
                    sudo systemctl start ${SERVICE_NAME}
                '''
            }
        }

        stage('Verify Service') {
            steps {
                sh '''
                    sudo systemctl is-active --quiet ${SERVICE_NAME}
                    if [ $? -ne 0 ]; then
                        echo "❌ Сервис ${SERVICE_NAME} не запущен!"
                        sudo systemctl status ${SERVICE_NAME} --no-pager
                        exit 1
                    fi
                    echo "✅ OK ${SERVICE_NAME}."
                '''
            }
        }
    }

    post {
        success {
            sh 'echo "udp-logger-client deployed successfully at $(date)" | logger -t jenkins'
            sh '''
                if [ "${REMOVE_REDIS_ON_FINISH}" = "true" ]; then
                    docker rm -f ${REDIS_CONTAINER} >/dev/null 2>&1 || true
                fi
            '''
        }
        failure {
            sh 'echo "udp-logger-client deployment failed at $(date)" | logger -t jenkins'
            sh '''
                if [ "${REMOVE_REDIS_ON_FINISH}" = "true" ]; then
                    docker rm -f ${REDIS_CONTAINER} >/dev/null 2>&1 || true
                fi
            '''
        }
    }
}

