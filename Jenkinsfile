@Library('shared-lib') _
// - Fetch Source Code          大约耗时
// - Sync source code           大约耗时
// - Build Docker Image         大约耗时
// - Parallel Cases
//  - Case-1: Style Check       大约耗时3分钟
//  - Case-2: Static Analyzer   大约耗时3分钟
//  - Case-3: Tests On ASan     大约耗时3分钟
//      - 3.1 ASan: Build
//      - 3.2 ASan: Parallel Tests
//          - ASan-Test-1: integration tests by script
//          - ASan-Test-2: statelest tests in docker
//          - ASan-Test-3: stateful tests in docker
//          - ASan-Test-4: unit tests by script
//  - Case-4: Tests On TSan     大约耗时3分钟
//  - Case-5: Tests On MSan     大约耗时3分钟
//  - Case-6: Tests On UbSan    大约耗时3分钟

def Sanitizer_Tests(String sanitizer, String id) {
    def TEST_TAG = "${BUILD_NUMBER}_${sanitizer}"
    if (id == '1') {
        return {
            // stage ("${sanitizer}-Test-1: integration tests by script") {
            //     sh "mkdir -p tests-${id} && rm -rf tests-${id}/* && cp -r ../tests/integration tests-${id}/"
            //     sh "mkdir -p reports && rm -rf reports/*"
            //     dir ("tests-${id}/integration") {
            //         sh "pytest --html=${WORKSPACE}/reports/${TEST_TAG}_IntegrationTest.html --self-contained-html"
            //     }
            // }
            stage ("${sanitizer}-Test-1: integration tests in docker") {
                sh "mkdir -p tests-${id} && rm -rf tests-${id}/* && cp -r ../tests/integration tests-${id}/"
                sh "mkdir -p reports && rm -rf reports/*"
                sh "docker system prune -f || true && docker run -i --rm --name ${TEST_TAG}_daisy_integration_tests --privileged -v $CLICKHOUSE_BIN_DIR:/programs -v $CLICKHOUSE_TESTS_BASE_CONFIG_DIR:/clickhouse-config -v ${WORKSPACE}/tests-${id}/integration:/ClickHouse/tests/integration -v $CLICKHOUSE_HOME/src/Server/grpc_protos:/ClickHouse/src/Server/grpc_protos -v ${WORKSPACE}/reports:/tests_output -v ${TEST_TAG}_clickhouse_integration_tests_volume:/var/lib/docker -e PYTEST_OPTS=\"--html=/tests_output/${TEST_TAG}_IntegrationTest.html --self-contained-html \" -e TEST_TAG=${TEST_TAG} daisy/clickhouse-integration-tests-runner"
            }
        }
    } else if (id == '2') {
        return {
            stage ("${sanitizer}-Test-2: statelest tests in docker") {
                sh "mkdir -p tests-${id} && rm -rf tests-${id}/* && cp -r ../tests/queries tests-${id}/"
                sh "mkdir -p reports && rm -rf reports/*"
                sh "docker system prune -f || true && docker run --net=none -i --rm --name ${TEST_TAG}_daisy_statelest_tests -v $CLICKHOUSE_BIN_DIR:/programs -v $CLICKHOUSE_TESTS_BASE_CONFIG_DIR/config.xml:/etc/clickhouse-server/config.xml -v $CLICKHOUSE_TESTS_BASE_CONFIG_DIR/users.xml:/etc/clickhouse-server/users.xml -v $CLICKHOUSE_HOME/tests/clickhouse-test:/usr/bin/clickhouse-test -v ${WORKSPACE}/tests-${id}/queries:/queries -v ${WORKSPACE}/reports:/tests_output -e TEST_TAG=${TEST_TAG} daisy/clickhouse-statelest-tests-runner 01677_array_enumerate_bug"
            }
        }
    } else if (id == '3') {
        return {
            stage ("${sanitizer}-Test-3: stateful tests in docker") {
                sh "mkdir -p tests-${id} && rm -rf tests-${id}/* && cp -r ../tests/queries tests-${id}/"
                sh "mkdir -p reports && rm -rf reports/*"
                sh "docker system prune -f || true && docker run --net=none -i --rm --name ${TEST_TAG}_daisy_stateful_tests -v $CLICKHOUSE_BIN_DIR:/programs -v $CLICKHOUSE_TESTS_BASE_CONFIG_DIR/config.xml:/etc/clickhouse-server/config.xml -v $CLICKHOUSE_TESTS_BASE_CONFIG_DIR/users.xml:/etc/clickhouse-server/users.xml -v $CLICKHOUSE_HOME/tests/clickhouse-test:/usr/bin/clickhouse-test -v ${WORKSPACE}/tests-${id}/queries:/queries -v ${WORKSPACE}/reports:/tests_output -e TEST_TAG=${TEST_TAG} daisy/clickhouse-stateful-tests-runner 00141_transform"
            }
        }
    } else if (id == '4') {
        return {
            stage ("${sanitizer}-Test-4: unit tests in docker") {
                sh "mkdir -p reports && rm -rf reports/*"
                sh "docker system prune -f || true && docker run --net=none -i --rm --name ${TEST_TAG}_daisy_unit_tests -v ${WORKSPACE}/build/src/unit_tests_dbms:/unit_tests_dbms -v ${WORKSPACE}/reports:/test_output -e TEST_TAG=${TEST_TAG} daisy/clickhouse-unit-tests-runner --gtest_filter=SequenceInfo.Merge"
            }
        }
    }
}

pipeline {
    agent any
    options {
        skipDefaultCheckout()
    }
    parameters {
        string(name: 'TESTS', defaultValue: '1,2,3,4', description: 'Tests ID')
    }
    stages {
        stage('Fetch Source Code') {
            agent { label 'bj' }
            steps {
                checkout scm
                archiveSource()
            }
        }
        stage ('Node "ph" Init') {
            agent { label 'ph' }
            stages {
                stage('"ph" Sync source code') {
                    steps {
                        echo "skip"
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        // 拷贝相关修改（临时）
                        sh "cp -r /data/wangjinlong1/jenkins-agent/workspace/lisen_bak/* ./"
                    }
                }
                stage('"ph" Build Docker Image') {
                    steps {
                        echo "skip"
                        sh "python3 utils/ci/build_images.py"
                    }
                }
            }
        }
        stage('Parallel Cases') {
            parallel {
                stage ('Case-1: Style Check') {
                    // 大约耗时3分钟
                    agent {
                        node {
                            label 'ph'
                            customWorkspace '/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd/CICD_Style_Check'
                        }
                    }
                    steps {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        // 拷贝相关修改（临时）
                        sh "cp -r /data/wangjinlong1/jenkins-agent/workspace/lisen_bak/* ./"

                        sh "mkdir -p reports && rm -rf reports/*"
                        sh "./utils/check-style/check-style-all | tee reports/${BUILD_NUMBER}_check-style-report.txt"
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                        }
                    }
                }

                stage ('Case-2: Static Analyzer') {
                    agent {
                        node {
                            label 'ph'
                            customWorkspace '/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd/CICD_Static_Analyzer'
                        }
                    }
                    steps {
                        echo "skip"
                        // fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        // // 拷贝相关修改（临时）
                        // sh "cp -r /data/wangjinlong1/jenkins-agent/workspace/lisen_bak/* ./"

                        // sh "mkdir -p reports && rm -rf reports/*"
                        // dir ("build_static_analyzer") {
                        //     sh "NINJA_FLAGS=-k0 cmake ../ -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DENABLE_CLANG_TIDY=1"
                        //     sh "NINJA_FLAGS=-k0 ninja -j8"
                        //     sh "scan-build-12 -v -V -o ../reports/result/scan-build-results ninja -j8 | tee ../reports/${BUILD_NUMBER}_scan-build-report.txt"
                        //     sh "codechecker analyze compile_commands.json -o ../reports/result"
                        //     sh "codechecker parse ../reports/result -e html -o ../reports/${BUILD_NUMBER}_codechecker-reports.html"
                        // }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                        }
                    }
                }

                stage ('Case-3: Tests On ASan') {
                    agent {
                        node {
                            label 'ph'
                            customWorkspace '/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd/CICD_Tests_On_ASan'
                        }
                    }
                    environment {
                        CLICKHOUSE_HOME="${WORKSPACE}/"
                        CLICKHOUSE_BIN_DIR="${WORKSPACE}/build/programs/"
                        CLICKHOUSE_TESTS_BASE_CONFIG_DIR="${WORKSPACE}/programs/server/"
                        // only for integration tests by script
                        CLICKHOUSE_TESTS_SERVER_BIN_PATH="${WORKSPACE}/build/programs/clickhouse"
                        CLICKHOUSE_TESTS_CLIENT_BIN_PATH="${WORKSPACE}/build/programs/clickhouse"
                        DOCKER_COMPOSE_DIR="${WORKSPACE}/docker/test/integration/runner/compose/"
                    }
                    stages {
                        stage ('3.1 ASan: Build') {
                            steps {
                                fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                                // 拷贝相关修改（临时）
                                sh "cp -r /data/wangjinlong1/jenkins-agent/workspace/lisen_bak/* ./"

                                dir ("build") {
                                    sh "cmake ../ -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DSANITIZE=address -DWITH_COVERAGE=ON && ninja -j8"
                                }
                            }
                        }
                        stage ('3.2 ASan: Parallel Tests') {
                            steps {
                                script {
                                    def tests = [:]
                                    for (id in params.TESTS.tokenize(',')) {
                                        tests.put("Test-" + id + " On ASan", Sanitizer_Tests('ASan', id))
                                    }
                                    parallel tests
                                }
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                        }
                        unsuccessful {
                            sh "docker rm -f ${BUILD_NUMBER}_ASan_daisy_integration_tests ${BUILD_NUMBER}_ASan_daisy_statelest_tests ${BUILD_NUMBER}_ASan_daisy_stateful_tests ${BUILD_NUMBER}_ASan_daisy_stateful_tests"
                        }
                    }
                }

                // stage ('Case-4: Tests On TSan') {
                //     agent {
                //         node {
                //             label 'ph'
                //             customWorkspace '/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd/CICD_Tests_On_TSan'
                //         }
                //     }
                //     environment {
                //         CLICKHOUSE_HOME="${WORKSPACE}/"
                //         CLICKHOUSE_BIN_DIR="${WORKSPACE}/build/programs/"
                //         CLICKHOUSE_TESTS_BASE_CONFIG_DIR="${WORKSPACE}/programs/server/"
                //         // only for integration tests by script
                //         CLICKHOUSE_TESTS_SERVER_BIN_PATH="${WORKSPACE}/build/programs/clickhouse"
                //         CLICKHOUSE_TESTS_CLIENT_BIN_PATH="${WORKSPACE}/build/programs/clickhouse"
                //         DOCKER_COMPOSE_DIR="${WORKSPACE}/docker/test/integration/runner/compose/"
                //     }
                //     stages {
                //         stage ('4.1 TSan: Build') {
                //             steps {
                //                 fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                //                 // 拷贝相关修改（临时）
                //                 sh "cp -r /data/wangjinlong1/jenkins-agent/workspace/lisen_bak/* ./"

                //                 dir ("build") {
                //                     sh "cmake ../ -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DSANITIZE=thread -DWITH_COVERAGE=ON && ninja -j8"
                //                 }
                //             }
                //         }
                //         stage ('4.2 TSan: Parallel Tests') {
                //             steps {
                //                 script {
                //                     def tests = [:]
                //                     for (id in params.TESTS.tokenize(',')) {
                //                         tests.put("Test-" + id + " On TSan", Sanitizer_Tests('TSan', id))
                //                     }
                //                     parallel tests
                //                 }
                //             }
                //         }
                //     }
                //     post {
                //         always {
                //             archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                //         }
                //         unsuccessful {
                //             sh "docker rm -f ${BUILD_NUMBER}_TSan_daisy_integration_tests ${BUILD_NUMBER}_TSan_daisy_statelest_tests ${BUILD_NUMBER}_TSan_daisy_stateful_tests ${BUILD_NUMBER}_TSan_daisy_stateful_tests"
                //         }
                //     }
                // }

                // stage ('Case-5: Tests On MSan') {
                //     agent {
                //         node {
                //             label 'ph'
                //             customWorkspace '/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd/CICD_Tests_On_MSan'
                //         }
                //     }
                //     environment {
                //         CLICKHOUSE_HOME="${WORKSPACE}/"
                //         CLICKHOUSE_BIN_DIR="${WORKSPACE}/build/programs/"
                //         CLICKHOUSE_TESTS_BASE_CONFIG_DIR="${WORKSPACE}/programs/server/"
                //         // only for integration tests by script
                //         CLICKHOUSE_TESTS_SERVER_BIN_PATH="${WORKSPACE}/build/programs/clickhouse"
                //         CLICKHOUSE_TESTS_CLIENT_BIN_PATH="${WORKSPACE}/build/programs/clickhouse"
                //         DOCKER_COMPOSE_DIR="${WORKSPACE}/docker/test/integration/runner/compose/"
                //     }
                //     stages {
                //         stage ('5.1 MSan: Build') {
                //             steps {
                //                 fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                //                 // 拷贝相关修改（临时）
                //                 sh "cp -r /data/wangjinlong1/jenkins-agent/workspace/lisen_bak/* ./"

                //                 dir ("build") {
                //                     sh "cmake ../ -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DSANITIZE=memory -DWITH_COVERAGE=ON && ninja -j8"
                //                 }
                //             }
                //         }
                //         stage ('5.2 MSan: Parallel Tests') {
                //             steps {
                //                 script {
                //                     def tests = [:]
                //                     for (id in params.TESTS.tokenize(',')) {
                //                         tests.put("Test-" + id + " On MSan", Sanitizer_Tests('MSan', id))
                //                     }
                //                     parallel tests
                //                 }
                //             }
                //         }
                //     }
                //     post {
                //         always {
                //             archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                //         }
                //         unsuccessful {
                //             sh "docker rm -f ${BUILD_NUMBER}_MSan_daisy_integration_tests ${BUILD_NUMBER}_MSan_daisy_statelest_tests ${BUILD_NUMBER}_MSan_daisy_stateful_tests ${BUILD_NUMBER}_MSan_daisy_stateful_tests"
                //         }
                //     }
                // }

                // stage ('Case-6: Tests On UbSan') {
                //     agent {
                //         node {
                //             label 'ph'
                //             customWorkspace '/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd/CICD_Tests_On_UbSan'
                //         }
                //     }
                //     environment {
                //         CLICKHOUSE_HOME="${WORKSPACE}/"
                //         CLICKHOUSE_BIN_DIR="${WORKSPACE}/build/programs/"
                //         CLICKHOUSE_TESTS_BASE_CONFIG_DIR="${WORKSPACE}/programs/server/"
                //         // only for integration tests by script
                //         CLICKHOUSE_TESTS_SERVER_BIN_PATH="${WORKSPACE}/build/programs/clickhouse"
                //         CLICKHOUSE_TESTS_CLIENT_BIN_PATH="${WORKSPACE}/build/programs/clickhouse"
                //         DOCKER_COMPOSE_DIR="${WORKSPACE}/docker/test/integration/runner/compose/"
                //     }
                //     stages {
                //         stage ('6.1 UbSan: Build') {
                //             steps {
                //                 fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                //                 // 拷贝相关修改（临时）
                //                 sh "cp -r /data/wangjinlong1/jenkins-agent/workspace/lisen_bak/* ./"

                //                 dir ("build") {
                //                     sh "cmake ../ -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DSANITIZE=undefined -DWITH_COVERAGE=ON && ninja -j8"
                //                 }
                //             }
                //         }
                //         stage ('6.2 UbSan: Parallel Tests') {
                //             steps {
                //                 script {
                //                     def tests = [:]
                //                     for (id in params.TESTS.tokenize(',')) {
                //                         tests.put("Test-" + id + " On UbSan", Sanitizer_Tests('UbSan', id))
                //                     }
                //                     parallel tests
                //                 }
                //             }
                //         }
                //     }
                //     post {
                //         always {
                //             archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                //         }
                //         unsuccessful {
                //             sh "docker rm -f ${BUILD_NUMBER}_UbSan_daisy_integration_tests ${BUILD_NUMBER}_UbSan_daisy_statelest_tests ${BUILD_NUMBER}_UbSan_daisy_stateful_tests ${BUILD_NUMBER}_UbSan_daisy_stateful_tests"
                //         }
                //     }
                // }
            }
        }
    }
}
