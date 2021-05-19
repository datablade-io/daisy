@Library('shared-lib') _
// - Fetch Source Code          大约耗时
// - Sync source code           大约耗时
// - Build Docker Image         大约耗时
// - Parallel Cases
//  - Case-1: Style Check       大约耗时3分钟
//  - Case-2: Static Analyzer   大约耗时
//  - Case-3: Tests On ASan     大约耗时
//      - 3.1 ASan: Build
//      - 3.2 ASan: Parallel Tests
//          - ASan-Test-1: integration tests in docker
//          - ASan-Test-2: statelest tests in docker
//          - ASan-Test-3: stateful tests in docker
//          - ASan-Test-4: unit tests in docker
//  - Case-4: Tests On TSan     大约耗时
//  - Case-5: Tests On MSan     大约耗时
//  - Case-6: Tests On UbSan    大约耗时

def Base_Tests(String base, String id) {
    def TEST_TAG = "${BUILD_NUMBER}_${base}"
    if (id == 'integration') {
        return {
            stage ("${base}-Test-Integration: integration tests in docker") {
                sh "mkdir -p tests-${id} && rm -rf tests-${id}/* && cp -r ../tests/integration tests-${id}/"
                sh "mkdir -p reports && rm -rf reports/*"
                sh "docker system prune -f || true && docker run -i --rm --name ${TEST_TAG}_daisy_integration_tests --privileged -v $CLICKHOUSE_BIN_DIR:/programs -v $CLICKHOUSE_TESTS_BASE_CONFIG_DIR:/clickhouse-config -v ${WORKSPACE}/tests-${id}/integration:/ClickHouse/tests/integration -v $CLICKHOUSE_HOME/src/Server/grpc_protos:/ClickHouse/src/Server/grpc_protos -v ${WORKSPACE}/reports:/tests_output -v ${TEST_TAG}_clickhouse_integration_tests_volume:/var/lib/docker -e PYTEST_OPTS=\"--html=/tests_output/${TEST_TAG}_IntegrationTest.html --self-contained-html \" -e TEST_TAG=${TEST_TAG} daisy/clickhouse-integration-tests-runner"
            }
        }
    } else if (id == 'statelest') {
        return {
            stage ("${base}-Test-Statelest: statelest tests in docker") {
                sh "mkdir -p tests-${id} && rm -rf tests-${id}/* && cp -r ../tests/queries tests-${id}/"
                sh "mkdir -p reports && rm -rf reports/*"
                sh "docker system prune -f || true && docker run --net=none -i --rm --name ${TEST_TAG}_daisy_statelest_tests -v $CLICKHOUSE_BIN_DIR:/programs -v $CLICKHOUSE_TESTS_BASE_CONFIG_DIR/config.xml:/etc/clickhouse-server/config.xml -v $CLICKHOUSE_TESTS_BASE_CONFIG_DIR/users.xml:/etc/clickhouse-server/users.xml -v $CLICKHOUSE_HOME/tests/clickhouse-test:/usr/bin/clickhouse-test -v ${WORKSPACE}/tests-${id}/queries:/queries -v ${WORKSPACE}/reports:/tests_output -e TEST_TAG=${TEST_TAG} daisy/clickhouse-statelest-tests-runner"
            }
        }
    } else if (id == 'stateful') {
        return {
            stage ("${base}-Test-Stateful: stateful tests in docker") {
                sh "mkdir -p tests-${id} && rm -rf tests-${id}/* && cp -r ../tests/queries tests-${id}/"
                sh "mkdir -p reports && rm -rf reports/*"
                sh "docker system prune -f || true && docker run --net=none -i --rm --name ${TEST_TAG}_daisy_stateful_tests -v $CLICKHOUSE_BIN_DIR:/programs -v $CLICKHOUSE_TESTS_BASE_CONFIG_DIR/config.xml:/etc/clickhouse-server/config.xml -v $CLICKHOUSE_TESTS_BASE_CONFIG_DIR/users.xml:/etc/clickhouse-server/users.xml -v $CLICKHOUSE_HOME/tests/clickhouse-test:/usr/bin/clickhouse-test -v ${WORKSPACE}/tests-${id}/queries:/queries -v ${WORKSPACE}/reports:/tests_output -e TEST_TAG=${TEST_TAG} daisy/clickhouse-stateful-tests-runner"
            }
        }
    } else if (id == 'unit') {
        return {
            stage ("${base}-Test-Unit: unit tests in docker") {
                sh "mkdir -p reports && rm -rf reports/*"
                sh "docker system prune -f || true && docker run --net=none -i --rm --name ${TEST_TAG}_daisy_unit_tests -v ${WORKSPACE}/build/src/unit_tests_dbms:/unit_tests_dbms -v ${WORKSPACE}/reports:/test_output -e TEST_TAG=${TEST_TAG} daisy/clickhouse-unit-tests-runner "
            }
        }
    } else {
        return {
            echo "unknown test \"${id}\""
        }
    }
}

pipeline {
    agent any
    options {
        skipDefaultCheckout()
    }
    parameters {
        string(name: 'TESTS', defaultValue: 'integration,statelest,stateful,unit', description: 'Tests Identifier')
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
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                    }
                }
                stage('"ph" Build Docker Image') {
                    steps {
                        sh "python3 utils/ci/build_images.py"
                    }
                }
            }
        }
        stage('Parallel Cases') {
            parallel {
                stage ('Case-1: Style Check') {
                    agent {
                        node {
                            label 'ph'
                            customWorkspace "/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd_${BUILD_NUMBER}/CICD_Style_Check"
                        }
                    }
                    steps {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)

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
                            customWorkspace '/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd_${BUILD_NUMBER}/CICD_Static_Analyzer'
                        }
                    }
                    steps {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)

                        sh "mkdir -p reports && rm -rf reports/*"
                        dir ("build_static_analyzer") {
                            // sh "NINJA_FLAGS=-k0 cmake ../ -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DENABLE_TESTS=OFF -DENABLE_CLANG_TIDY=1"
                            // sh "NINJA_FLAGS=-k0 ninja -j8"
                            // sh "scan-build-12 -v -V -o ../reports/result/scan-build-results ninja -j8 | tee ../reports/${BUILD_NUMBER}_scan-build-report.txt"
                            // sh "codechecker analyze compile_commands.json -o ../reports/result"
                            // sh "codechecker parse ../reports/result -e html -o ../reports/${BUILD_NUMBER}_codechecker-reports.html"
                        }
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
                            customWorkspace "/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd_${BUILD_NUMBER}/CICD_Tests_On_ASan"
                        }
                    }
                    environment {
                        CLICKHOUSE_HOME="${WORKSPACE}/"
                        CLICKHOUSE_BIN_DIR="${WORKSPACE}/build/programs/"
                        CLICKHOUSE_TESTS_BASE_CONFIG_DIR="${WORKSPACE}/programs/server/"
                    }
                    stages {
                        stage ('3.1 ASan: Build') {
                            steps {
                                fetchSource(env.JOB_NAME, env.BUILD_NUMBER)

                                dir ("build") {
                                    sh "cmake ../ -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DSANITIZE=address && ninja -j8"
                                }
                            }
                        }
                        stage ('3.2 ASan: Parallel Tests') {
                            steps {
                                script {
                                    def tests = [:]
                                    for (id in params.TESTS.tokenize(',')) {
                                        tests.put("Test-" + id + " On ASan", Base_Tests('ASan', id))
                                    }
                                    parallel tests
                                }
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                            sh "docker rm -f ${BUILD_NUMBER}_ASan_daisy_integration_tests ${BUILD_NUMBER}_ASan_daisy_statelest_tests ${BUILD_NUMBER}_ASan_daisy_stateful_tests ${BUILD_NUMBER}_ASan_daisy_stateful_tests || true"
                        }
                    }
                }

                stage ('Case-4: Tests On TSan') {
                    agent {
                        node {
                            label 'ph'
                            customWorkspace "/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd_${BUILD_NUMBER}/CICD_Tests_On_TSan"
                        }
                    }
                    environment {
                        CLICKHOUSE_HOME="${WORKSPACE}/"
                        CLICKHOUSE_BIN_DIR="${WORKSPACE}/build/programs/"
                        CLICKHOUSE_TESTS_BASE_CONFIG_DIR="${WORKSPACE}/programs/server/"
                    }
                    stages {
                        stage ('4.1 TSan: Build') {
                            steps {
                                fetchSource(env.JOB_NAME, env.BUILD_NUMBER)

                                dir ("build") {
                                    sh "cmake ../ -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DSANITIZE=thread && ninja -j8"
                                }
                            }
                        }
                        stage ('4.2 TSan: Parallel Tests') {
                            steps {
                                script {
                                    def tests = [:]
                                    for (id in params.TESTS.tokenize(',')) {
                                        tests.put("Test-" + id + " On TSan", Base_Tests('TSan', id))
                                    }
                                    parallel tests
                                }
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                            sh "docker rm -f ${BUILD_NUMBER}_TSan_daisy_integration_tests ${BUILD_NUMBER}_TSan_daisy_statelest_tests ${BUILD_NUMBER}_TSan_daisy_stateful_tests ${BUILD_NUMBER}_TSan_daisy_stateful_tests || true"
                        }
                    }
                }

                stage ('Case-5: Tests On MSan') {
                    agent {
                        node {
                            label 'ph'
                            customWorkspace "/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd_${BUILD_NUMBER}/CICD_Tests_On_MSan"
                        }
                    }
                    environment {
                        CLICKHOUSE_HOME="${WORKSPACE}/"
                        CLICKHOUSE_BIN_DIR="${WORKSPACE}/build/programs/"
                        CLICKHOUSE_TESTS_BASE_CONFIG_DIR="${WORKSPACE}/programs/server/"
                    }
                    stages {
                        stage ('5.1 MSan: Build') {
                            steps {
                                fetchSource(env.JOB_NAME, env.BUILD_NUMBER)

                                dir ("build") {
                                    sh "cmake ../ -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DSANITIZE=memory && ninja -j8"
                                }
                            }
                        }
                        stage ('5.2 MSan: Parallel Tests') {
                            steps {
                                script {
                                    def tests = [:]
                                    for (id in params.TESTS.tokenize(',')) {
                                        tests.put("Test-" + id + " On MSan", Base_Tests('MSan', id))
                                    }
                                    parallel tests
                                }
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                            sh "docker rm -f ${BUILD_NUMBER}_MSan_daisy_integration_tests ${BUILD_NUMBER}_MSan_daisy_statelest_tests ${BUILD_NUMBER}_MSan_daisy_stateful_tests ${BUILD_NUMBER}_MSan_daisy_stateful_tests || true"
                        }
                    }
                }

                stage ('Case-6: Tests On UbSan') {
                    agent {
                        node {
                            label 'ph'
                            customWorkspace "/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd_${BUILD_NUMBER}/CICD_Tests_On_UbSan"
                        }
                    }
                    environment {
                        CLICKHOUSE_HOME="${WORKSPACE}/"
                        CLICKHOUSE_BIN_DIR="${WORKSPACE}/build/programs/"
                        CLICKHOUSE_TESTS_BASE_CONFIG_DIR="${WORKSPACE}/programs/server/"
                    }
                    stages {
                        stage ('6.1 UbSan: Build') {
                            steps {
                                fetchSource(env.JOB_NAME, env.BUILD_NUMBER)

                                dir ("build") {
                                    sh "cmake ../ -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DSANITIZE=undefined && ninja -j8"
                                }
                            }
                        }
                        stage ('6.2 UbSan: Parallel Tests') {
                            steps {
                                script {
                                    def tests = [:]
                                    for (id in params.TESTS.tokenize(',')) {
                                        tests.put("Test-" + id + " On UbSan", Base_Tests('UbSan', id))
                                    }
                                    parallel tests
                                }
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                            sh "docker rm -f ${BUILD_NUMBER}_UbSan_daisy_integration_tests ${BUILD_NUMBER}_UbSan_daisy_statelest_tests ${BUILD_NUMBER}_UbSan_daisy_stateful_tests ${BUILD_NUMBER}_UbSan_daisy_stateful_tests || true"
                        }
                    }
                }

                stage ('Case-7: Tests For Coverage') {
                    agent {
                        node {
                            label 'ph'
                            customWorkspace "/data/wangjinlong1/jenkins-agent/workspace/Daisy-CICD_lisen_test_cicd_${BUILD_NUMBER}/CICD_Tests_For_Coverage"
                        }
                    }
                    environment {
                        CLICKHOUSE_HOME="${WORKSPACE}/"
                        CLICKHOUSE_BIN_DIR="${WORKSPACE}/build/programs/"
                        CLICKHOUSE_TESTS_BASE_CONFIG_DIR="${WORKSPACE}/programs/server/"
                    }
                    stages {
                        stage ('7.1 Coverage: Build') {
                            steps {
                                fetchSource(env.JOB_NAME, env.BUILD_NUMBER)

                                dir ("build") {
                                    sh "cmake ../ -DCMAKE_BUILD_TYPE=Debug -DCMAKE_C_COMPILER=/usr/bin/clang-12 -DCMAKE_CXX_COMPILER=/usr/bin/clang++-12 -DWITH_COVERAGE=ON && ninja -j8"
                                }
                            }
                        }
                        stage ('7.2 Coverage: Parallel Tests') {
                            steps {
                                script {
                                    def tests = [:]
                                    for (id in params.TESTS.tokenize(',')) {
                                        tests.put("Test-" + id + " For Coverage", Base_Tests('Coverage', id))
                                    }
                                    parallel tests
                                }
                            }
                        }
                        stage ('7.3 Coverage: Generate Coverage Report') {
                            steps {
                                withEnv(["COVERAGE_DIR=reports/${BUILD_NUMBER}_coverage_reports"]) {
                                    sh "mkdir -p ${COVERAGE_DIR} && find . -path './reports' -prune -o -name '*.profraw' | xargs -i cp --force --backup=numbered {} ${COVERAGE_DIR}/ | true"
                                    sh "llvm-profdata-12 merge -sparse ${COVERAGE_DIR}/*.profraw -o reports/coverage_reports/clickhouse.profdata"
                                    sh "llvm-cov-12 export ${CLICKHOUSE_TESTS_SERVER_BIN_PATH} -instr-profile=${COVERAGE_DIR}/clickhouse.profdata -j=16 -format=lcov -ignore-filename-regex '.*contrib.*' > ${COVERAGE_DIR}/output.lcov"
                                    sh "genhtml ${COVERAGE_DIR}/output.lcov --ignore-errors source --output-directory \"${COVERAGE_DIR}/html/\""
                                    sh "cp ${COVERAGE_DIR}/html/index.html reports/${BUILD_NUMBER}_coverage_index.html && tar -czvf reports/${BUILD_NUMBER}_coverage_reports.tar.gz ${COVERAGE_DIR}/html/*"
                                }
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                            sh "docker rm -f ${BUILD_NUMBER}_Coverage_daisy_integration_tests ${BUILD_NUMBER}_Coverage_daisy_statelest_tests ${BUILD_NUMBER}_Coverage_daisy_stateful_tests ${BUILD_NUMBER}_Coverage_daisy_stateful_tests || true"
                        }
                    }
                }
            }
        }
    }
}
