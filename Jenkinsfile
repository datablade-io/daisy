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
                archiveSource()
            }
        }

        stage('Builder') {
            agent { label 'builder'}
            stages {
                stage('Build Docker Image') {
                    steps {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        sh "python3 utils/ci/build_images.py"
                    }
                }
                /* 
                stage('style check') {
                    steps {
                        sh "[ -d \"./reports/check-style\" ] || rm -rf reports"
                        sh "mkdir -p reports/check-style"
                        sh "./utils/check-style/check-style-all > reports/check-style/report.txt"
                        archiveArtifacts artifacts: 'reports/check-style/report.txt', followSymlinks: false
                    }
                }
                */
                stage('static analyzer') {
                    steps {
                        dir("build_static_analyzer") {
                            sh "NINJA_FLAGS=-k0 cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DENABLE_CLANG_TIDY=1 -DWITH_COVERAGE=ON -DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache"
                            sh "NINJA_FLAGS=-k0 ninja -j200"
                            sh "scan-build-12 -v -V -o scan-build-results ninja -j200"
                            sh "codechecker analyze compile_commands.json -o ../reports/codechecker"
                            sh "codechecker parse ../reports/codechecker -e html -o ../reports/codechecker/reports_html"
                            archiveArtifacts artifacts: 'reports/codechecker/reports_html/*', followSymlinks: false
                        }
                    }
                }

                stage ('ASan') {
                    steps{
                        dir("build_asan") {
                            sh "cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DSANITIZE=address -DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache && ninja -j200"
                        }
                        echo '$WORKSPACE'
                        withEnv(['CLICKHOUSE_HOME=$WORKSPACE',
                        'CLICKHOUSE_BIN_DIR=$WORKSPACE/build_asan/programs/',
                        'CLICKHOUSE_TESTS_SERVER_BIN_PATH=$WORKSPACE/build_asan/programs/clickhouse',
                        'CLICKHOUSE_TESTS_CLIENT_BIN_PATH=$WORKSPACE/build_asan/programs/clickhouse',
                        'CLICKHOUSE_TESTS_BASE_CONFIG_DIR=$WORKSPACE/programs/server/',
                        'DOCKER_COMPOSE_DIR=$WORKSPACE/docker/test/integration/runner/compose/']) {
                            dir('tests/integration') {
                                sh "pytest --html=IntegrationTest.html --self-contained-html test_default_role"
                            }
                        }
                    }
                }


            }
        }
    }
}
