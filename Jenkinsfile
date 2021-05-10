@Library('shared-lib') _

pipeline {
    agent any
    options {
        skipDefaultCheckout()
    }

    stages {
        stage('Fetch Source Code') {
            agent { label 'bj' }
            steps {
                checkout scm
                sh "git submodule update --init --recursive"
                archiveSource()
            }
        }

        stage('Build Docker Image') {
            agent { label 'ph'}
            steps {
                fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                sh "python3 utils/ci/build_images.py"
            }
        }

        stage('Build Binary') {
            steps {
                echo "build in docker, and publish docker image with different tag"
            }
        }
    }
}