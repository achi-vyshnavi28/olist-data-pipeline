// Jenkins declarative pipeline: same stages as the GitHub Actions workflow, for teams that run Jenkins.
// Needs an agent with Docker and Java 17; the pipeline itself runs inside the project's Docker image.
pipeline {
    agent any
    options {
        timeout(time: 45, unit: 'MINUTES')
        buildDiscarder(logRotator(numToKeepStr: '20'))
    }
    environment {
        IMAGE = "olist-pipeline:${env.BUILD_NUMBER}"
    }
    stages {
        stage('Install') {
            steps {
                sh 'python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt'
            }
        }
        stage('Unit and streaming tests') {
            steps {
                sh '. .venv/bin/activate && pytest -q --junitxml=reports/junit.xml'
            }
            post {
                always { junit 'reports/junit.xml' }
            }
        }
        stage('Batch pipeline on sample') {
            steps {
                sh '. .venv/bin/activate && python -m pipeline.run'
                archiveArtifacts artifacts: 'lake/reports/*.json', fingerprint: true
            }
        }
        stage('Quality gate') {
            steps {
                sh '''. .venv/bin/activate && python -c "import json,sys; s=json.load(open('lake/reports/quality_gold.json')); sys.exit(1 if s['failed_errors'] else 0)"'''
            }
        }
        stage('Docker image') {
            when { branch 'main' }
            steps {
                sh 'docker build -t $IMAGE . && docker run --rm $IMAGE'
            }
        }
    }
    post {
        failure { echo 'Pipeline failed: check the quality report and test results.' }
    }
}
