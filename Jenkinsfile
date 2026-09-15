@Library('rafay-builder@main') _
pipeline {
    environment {
      karpenterProviderRegistry = "registry.dev.rafay-edge.net/rafay/karpenter-provider-rafay"
      DOCKER_BUILDKIT='1'
      registryCredential = 'rcloud_user_registry.stage.rafay.cloud'
      registryUrl= 'https://registry.dev.rafay-edge.net'
      registry = 'registry.dev.rafay-edge.net'
      repository = 'rafay'
      dockerImage = ''
      prismaPassword=credentials('prisma')
      prismaUser = '07d05714-1ae0-4393-ba39-220052f69a2b'
    }
  agent {
    label params.USE_FIPS_NODE ? 'fips-node' : 'ec2-fleet'
  }
  parameters {
    booleanParam(name: 'USE_FIPS_NODE', defaultValue: false, description: 'If true, use fips-node. Otherwise, use ec2-fleet.')
  }
    options {
        ansiColor('xterm')
    }
    stages {
      stage('Sonar Analysis') {
        steps {
          script {
            def scannerHome = tool 'SonarScanner'
            withSonarQubeEnv() {
              sh "${scannerHome}/bin/sonar-scanner"
            }
          }
        }
      }
      stage('Quality Gate') {
        steps {
          timeout(time: 5, unit: 'MINUTES') {
            waitForQualityGate abortPipeline: true
          }
        }
      }
      stage('karpenter-provider-rafay image building, scanning and pushing.') {
        stages {
          stage('karpenter-provider-rafay image building and scanning.') {
            steps {
              script {
                def results = buildAndScan(
                  registryUrl: env.registryUrl,
                  registryCredential: env.registryCredential,
                  prismaUser: env.prismaUser,
                  imageName: "karpenter-provider-rafay",
                  prismaPassword: env.prismaPassword,
                  USE_FIPS_NODE: params.USE_FIPS_NODE,
                  dockerfile: "./Dockerfile",
                  dockerfileFips: "./Dockerfile.fips",
                  dockfileContextPath: "."
                )
                env.KARPENTER_PROVIDER_SCAN_RESULT = results.scanResult
                echo "Build Scan result: ${env.KARPENTER_PROVIDER_SCAN_RESULT}"
              }
            }
          }
          stage('karpenter-provider-rafay image pushing') {
            steps {
              script {
                echo "Pushing image to registry:-" + env.KARPENTER_PROVIDER_SCAN_RESULT
                pushImage(
                  registryUrl: env.registryUrl,
                  registryCredential: env.registryCredential,
                  imageName: "karpenter-provider-rafay",
                  USE_FIPS_NODE: params.USE_FIPS_NODE,
                  scanResult: env.KARPENTER_PROVIDER_SCAN_RESULT,
                  dockerfile: "./Dockerfile",
                  dockerfileFips: "./Dockerfile.fips",
                  dockfileContextPath: "."
                )
              }
            }
            post {
              success {
                script {
                  removeImage(
                    registry: env.registry,
                    repository: env.repository,
                    imageName: "karpenter-provider-rafay"
                  )
                }
              }
            }
          }
        }
      }
    }
    post {
        success {
            slackSend channel: "#build",
            color: 'good',
            message: "Build ${currentBuild.fullDisplayName} completed successfully."
        }
        failure {
            slackSend channel: "#build",
            color: 'RED',
            message: "Attention ${env.JOB_NAME} ${env.BUILD_NUMBER} has failed."
        }
        always {
            deleteDir()
            dir("${workspace}@tmp") {
                deleteDir()
            }
            dir("${workspace}@script") {
                deleteDir()
            }
        }
    }
}
