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
        }
        failure {
            sh 'echo "udp-logger-client deployment failed at $(date)" | logger -t jenkins'
        }
    }
}

