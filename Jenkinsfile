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

ExistedContainers = []

def Base_Tests(String base, String id)
{
    def TEST_TAG = "${BUILD_NUMBER}_${base}"

    // template 'runners'
    def runners = 
    [
        'integration' :
        [
            'stage_name' : "${base}-Test-Integration: integration tests in docker",
            'image' : docker.image("daisy/clickhouse-integration-tests-runner:${params.TESTS_IMAGE_TAG}"),
            'name' : "${TEST_TAG}_daisy_integration_tests_runner",
            'args' : "--privileged -e PYTEST_OPTS=\"--html=/tests_output/${TEST_TAG}_IntegrationTest.html --self-contained-html \" -e TEST_TAG=${TEST_TAG}",
            'loaders' :
            [
                'bin' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}"),
                    'name' : "${TEST_TAG}_daisy_integration_tests_runner-bin-loader",
                    'volume' : "/programs",
                    'inside_cmd' : [ 'cp -ar /usr/bin/clickhouse* /programs/' ]
                ],
                'config' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}"),
                    'name' : "${TEST_TAG}_daisy_integration_tests_runner-config-loader",
                    'volume' : '/usr/share/clickhouse-test/config'
                ],
                'tests' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}"),
                    'name' : "${TEST_TAG}_daisy_integration_tests_runner-tests-loader",
                    'volume' : "/usr/share/clickhouse-test/integration"
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [ 
                'cp -ar /programs/* /usr/bin/',
                '/usr/share/clickhouse-test/config/install.sh',
                'ln -sf /usr/share/clickhouse-test/config/config.xml /etc/clickhouse-server/',
                'ln -sf /usr/share/clickhouse-test/config/users.xml /etc/clickhouse-server/',
                'dockerd-entrypoint.sh'
            ]
        ],
        'statelest' :
        [
            'stage_name' : "${base}-Test-Statelest: statelest tests in docker",
            'image' : docker.image("daisy/clickhouse-statelest-tests-runner:${params.TESTS_IMAGE_TAG}"),
            'name' : "${TEST_TAG}_daisy_statelest_tests_runner",
            'args' : "--net=none -e TEST_TAG=${TEST_TAG}",
            'loaders' :
            [
                'bin' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}"),
                    'name' : "${TEST_TAG}_daisy_statelest_tests_runner-bin-loader",
                    'volume' : "/programs",
                    'inside_cmd' : [ 'cp -ar /usr/bin/clickhouse* /programs/' ]
                ],
                'config' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}"),
                    'name' : "${TEST_TAG}_daisy_statelest_tests_runner-config-loader",
                    'volume' : '/usr/share/clickhouse-test/config',
                    'inside_cmd' : [ 'cp /etc/clickhouse-server/*.xml /usr/share/clickhouse-test/config/' ]
                ],
                'tests' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}"),
                    'name' : "${TEST_TAG}_daisy_statelest_tests_runner-tests-loader",
                    'volume' : "/usr/share/clickhouse-test/queries"
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [ 
                'cp -ar /programs/* /usr/bin/',
                '/usr/share/clickhouse-test/config/install.sh',
                'ln -sf /usr/share/clickhouse-test/config/config.xml /etc/clickhouse-server/',
                'ln -sf /usr/share/clickhouse-test/config/users.xml /etc/clickhouse-server/',
                'chown -R $(whoami) /var/lib/clickhouse',
                'dockerd-entrypoint.sh'
            ]
        ],
        'stateful' :
        [
            'stage_name' : "${base}-Test-Stateful: stateful tests in docker",
            'image' : docker.image("daisy/clickhouse-stateful-tests-runner:${params.TESTS_IMAGE_TAG}"),
            'name' : "${TEST_TAG}_daisy_stateful_tests_runner",
            'args' : "--net=none -e TEST_TAG=${TEST_TAG}",
            'loaders' :
            [
                'bin' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}"),
                    'name' : "${TEST_TAG}_daisy_stateful_tests_runner-bin-loader",
                    'volume' : "/programs",
                    'inside_cmd' : [ 'cp -ar /usr/bin/clickhouse* /programs/' ]
                ],
                'config' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}"),
                    'name' : "${TEST_TAG}_daisy_stateful_tests_runner-config-loader",
                    'volume' : '/usr/share/clickhouse-test/config',
                    'inside_cmd' : [ 'cp /etc/clickhouse-server/*.xml /usr/share/clickhouse-test/config/' ]
                ],
                'tests' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}"),
                    'name' : "${TEST_TAG}_daisy_stateful_tests_runner-tests-loader",
                    'volume' : "/usr/share/clickhouse-test/queries"
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [ 
                'cp -ar /programs/* /usr/bin/',
                '/usr/share/clickhouse-test/config/install.sh',
                'ln -sf /usr/share/clickhouse-test/config/config.xml /etc/clickhouse-server/',
                'ln -sf /usr/share/clickhouse-test/config/users.xml /etc/clickhouse-server/',
                'chown -R $(whoami) /var/lib/clickhouse',
                'dockerd-entrypoint.sh'
            ]
        ],
        'unit' :
        [
            'stage_name' : "${base}-Test-Unit: unit tests in docker",
            'image' : docker.image("daisy/clickhouse-unit-tests-runner:${params.TESTS_IMAGE_TAG}"),
            'name' : "${TEST_TAG}_daisy_unit_tests_runner",
            'args' : "--net=none -e TEST_TAG=${TEST_TAG}",
            'loaders' :
            [
                'bin' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}"),
                    'name' : "${TEST_TAG}_daisy_unit_tests_runner-bin-loader",
                    'volume' : "/programs",
                    'inside_cmd' : [ 'cp -r /usr/bin/unit_tests_dbms /programs/' ]
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [ 
                'cp -ar /programs/* /usr/bin/',
                'dockerd-entrypoint.sh'
            ]
        ]
    ]

    def test_runner = runners[id]
    return test_runner ?
    {  /// 返回对应测试stage
        stage ("${test_runner.stage_name}")
        {
            def runner = test_runner
            def loaders = test_runner.loaders?.values()
            def f =
            {
                if (!loaders.isEmpty()) {  /// 前置加载
                    def loader = loaders.pop()
                    assert loader.image && loader.name && loader.volume
                    def c = loader.image.run(" -u root -it --entrypoint='' --name ${loader.name} -v ${loader.volume}", 'cat')
                    ExistedContainers.push(c)
                    loader.inside_cmd?.each { item -> sh "docker exec ${c.id} sh -c \"${item}\"" }
                    sh "docker stop ${c.id}"
                    runner.args += " --volumes-from ${loader.name} "
                    call()
                } else {   /// 执行测试
                    assert runner.image && runner.name
                    if (runner.reports) {
                        sh "mkdir -p ${WORKSPACE}/reports"
                        runner.args += " -v ${WORKSPACE}/reports:${runner.reports} "
                    }
                    runner.image.inside(runner.args + " -u root --entrypoint='' --name ${runner.name} ") {
                        runner.inside_cmd?.each { item -> sh "${item}" }
                    }
                }
            }()
        }
    }
    :
    {  /// 未知测试id
        echo "Unknown \'${id}\'' Test"
    }
}

pipeline {
    agent any
    options {
        skipDefaultCheckout()
    }

    parameters {
        string(name: 'TESTS', defaultValue: 'integration,statelest,stateful,unit', description: 'Tests 测试项标识符')
        string(name: 'TESTS_IMAGE_TAG', defaultValue: env.BUILD_NUMBER, description: '测试镜像TAG e.g. latest BUILD_NUMBER 100')
    }
    stages {
        stage('Fetch Source Code') {
            agent { label 'bj' }
            steps {
                // echo "skip"
                checkout scm
                archiveSource()
            }
        }
        stage ('Build Image') {
            agent {
                label 'ph'
            }
            steps {
                // echo "skip"
                fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                script {
                    docker.withRegistry('https://cicddockerhub.com:5000') {
                        docker.build("daisy/clickhouse-tests-env:${params.TESTS_IMAGE_TAG}", "/data/jenkins/workspace/deb").push()
                        docker.build("daisy/clickhouse-integration-tests-runner:${params.TESTS_IMAGE_TAG}", "${WORKSPACE}/docker/test/integration/runner/").push()
                        docker.build("daisy/clickhouse-statelest-tests-runner:${params.TESTS_IMAGE_TAG}", "${WORKSPACE}/docker/test/stateless_unbundled/runner/").push()
                        docker.build("daisy/clickhouse-stateful-tests-runner:${params.TESTS_IMAGE_TAG}", "--build-arg TAG=${params.TESTS_IMAGE_TAG} ${WORKSPACE}/docker/test/stateful/runner/").push()
                        docker.build("daisy/clickhouse-unit-tests-runner:${params.TESTS_IMAGE_TAG}", "${WORKSPACE}/docker/test/unit/runner/").push()
                    }
                }
            }
        }
        stage ('Parallel Cases') {
            parallel {
                stage ('Case-3: Tests On ASan') {
                    agent {
                        label 'ph'
                    }
                    steps {
                        script {
                            docker.withRegistry('https://cicddockerhub.com:5000') {
                                // sh "docker system prune -f || true"
                                def tests = [:]
                                for (id in params.TESTS.tokenize(',')) {
                                    tests.put("Test-" + id + " On ASan", Base_Tests('ASan', id))
                                }
                                parallel tests
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                            sh "rm -rf reports/*.*"
                            script {
                                ExistedContainers.each { it.stop() }  /// 该操作等价于'docker stop && docker rm'
                            }
                        }
                    }
                }
            }
        }
    }
}