pipeline {
    agent any

    environment {
        MAP_KEY = credentials('MAP_KEY')
    }

    stages {
        stage('Build') {
            steps {
                sh 'docker compose build'
                sh 'docker build -t firms-tests ./tests'
            }
        }

        stage('Test') {
            steps {
                sh 'docker compose up -d mongo api'
                sh 'sleep 5'
                sh 'docker run --rm --network firms_network firms-tests'
            }
        }

        stage('Deploy') {
            steps {
                sh 'docker compose up -d --build'
            }
        }
    }

    post {
        always {
            sh 'docker compose ps'
        }
        failure {
            echo 'El pipeline fallo: revisar la etapa de Test antes de desplegar.'
        }
    }
}
