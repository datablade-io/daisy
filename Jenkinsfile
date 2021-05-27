@Library('shared-lib') _

ExistedContainers = []

def Base_Tests(String tag, String id)
{
    /// params.TESTS_TAG:测试任务标示 tag:测试类型标示 utag:全局测试类型标示（用于容器名）
    def utag = "${tag}-${params.TESTS_TAG}"
    // template 'runners'
    def runners = 
    [
        'integration' :
        [
            'stage_name' : "${tag}-Test-Integration: integration tests in docker",
            'image' : docker.image("daisy/clickhouse-integration-tests-runner:${params.TESTS_TAG}"),
            'name' : "${utag}_daisy_integration_tests_runner",
            'args' : "--privileged -e PYTEST_OPTS=\"--html=/tests_output/${tag}_IntegrationTest.html --self-contained-html \"",
            'loaders' :
            [
                'bin' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_TAG}"),
                    'name' : "${utag}_daisy_integration_tests_runner-bin-loader",
                    'volume' : "/clickhouse-tests-env/${tag}-bin"
                ],
                'config' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_TAG}"),
                    'name' : "${utag}_daisy_integration_tests_runner-config-loader",
                    'volume' : '/clickhouse-tests-env/config'
                ],
                'tests' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_TAG}"),
                    'name' : "${utag}_daisy_integration_tests_runner-tests-loader",
                    'volume' : "/clickhouse-tests-env/integration"  // 注意保证integration内部不存在link
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [
                "cp -ar /clickhouse-tests-env/${tag}-bin/* /usr/bin/",
                '/clickhouse-tests-env/config/install.sh',
                'ln -sf /clickhouse-tests-env/config/config.xml /etc/clickhouse-server/',
                'ln -sf /clickhouse-tests-env/config/users.xml /etc/clickhouse-server/',
                'dockerd-entrypoint.sh sh -c "pytest $PYTEST_OPTS"'
            ]
        ],
        'statelest' :
        [
            'stage_name' : "${tag}-Test-Statelest: statelest tests in docker",
            'image' : docker.image("daisy/clickhouse-statelest-tests-runner:${params.TESTS_TAG}"),
            'name' : "${utag}_daisy_statelest_tests_runner",
            'args' : "--net=none -e TEST_TAG=${tag}",
            'loaders' :
            [
                'bin' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_TAG}"),
                    'name' : "${utag}_daisy_statelest_tests_runner-bin-loader",
                    'volume' : "/clickhouse-tests-env/${tag}-bin"
                ],
                'config' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_TAG}"),
                    'name' : "${utag}_daisy_statelest_tests_runner-config-loader",
                    'volume' : '/clickhouse-tests-env/config'
                ],
                'tests' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_TAG}"),
                    'name' : "${utag}_daisy_statelest_tests_runner-tests-loader",
                    'volume' : "/clickhouse-tests-env/queries"
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [ 
                "cp -ar /clickhouse-tests-env/${tag}-bin/* /usr/bin/",
                '/clickhouse-tests-env/config/install.sh',
                'ln -sf /clickhouse-tests-env/config/config.xml /etc/clickhouse-server/',
                'ln -sf /clickhouse-tests-env/config/users.xml /etc/clickhouse-server/',
                'chown -R $(whoami) /var/lib/clickhouse',
                'dockerd-entrypoint.sh'
            ]
        ],
        'stateful' :
        [
            'stage_name' : "${tag}-Test-Stateful: stateful tests in docker",
            'image' : docker.image("daisy/clickhouse-stateful-tests-runner:${params.TESTS_TAG}"),
            'name' : "${utag}_daisy_stateful_tests_runner",
            'args' : "--net=none -e TEST_TAG=${tag}",
            'loaders' :
            [
                'bin' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_TAG}"),
                    'name' : "${tag}_daisy_stateful_tests_runner-bin-loader",
                    'volume' : "/clickhouse-tests-env/${tag}-bin"
                ],
                'config' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_TAG}"),
                    'name' : "${utag}_daisy_stateful_tests_runner-config-loader",
                    'volume' : '/clickhouse-tests-env/config'
                ],
                'tests' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_TAG}"),
                    'name' : "${utag}_daisy_stateful_tests_runner-tests-loader",
                    'volume' : "/clickhouse-tests-env/queries"
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [ 
                "cp -ar /clickhouse-tests-env/${tag}-bin/* /usr/bin/",
                '/clickhouse-tests-env/config/install.sh',
                'ln -sf /clickhouse-tests-env/config/config.xml /etc/clickhouse-server/',
                'ln -sf /clickhouse-tests-env/config/users.xml /etc/clickhouse-server/',
                'chown -R $(whoami) /var/lib/clickhouse',
                'dockerd-entrypoint.sh'
            ]
        ],
        'unit' :
        [
            'stage_name' : "${tag}-Test-Unit: unit tests in docker",
            'image' : docker.image("daisy/clickhouse-unit-tests-runner:${params.TESTS_TAG}"),
            'name' : "${utag}_daisy_unit_tests_runner",
            'args' : "--net=none -e TEST_TAG=${tag}",
            'loaders' :
            [
                'bin' :
                [
                    'image' : docker.image("daisy/clickhouse-tests-env:${params.TESTS_TAG}"),
                    'name' : "${utag}_daisy_unit_tests_runner-bin-loader",
                    'volume' : "/clickhouse-tests-env/${tag}-bin"
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [ 
                "cp -ar /clickhouse-tests-env/${tag}-bin/* /usr/bin/",
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
                if (!loaders.isEmpty())
                {  /// 前置加载
                    def loader = loaders.pop()
                    if (loader.image)
                    {   /// 挂载镜像容器卷
                        assert loader.name && loader.volume
                        sh "docker inspect -f . ${loader.image.id} || docker pull registry.foundary.zone:8360/${loader.image.id} && docker tag registry.foundary.zone:8360/${loader.image.id} ${loader.image.id}"
                        def c = loader.image.run(" -u root -it --entrypoint='' --name ${loader.name} -v ${loader.volume}", 'cat')
                        ExistedContainers.push(c)
                        loader.inside_cmd?.each { item -> sh "docker exec ${c.id} sh -c \"${item}\"" }
                        sh "docker stop ${c.id}"
                        runner.args += " --volumes-from ${loader.name} "
                    }
                    else
                    {   /// 挂载本地卷
                        assert loader.volume
                        runner.args += " -v ${loader.volume} "
                    }
                    call()
                }
                else
                {   /// 执行测试
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
        stage("Unknown \'${id}\'' Test") {
            echo "skip"
        }
    }
}

pipeline {
    agent any
    options {
        skipDefaultCheckout()
        timestamps()

    }
    parameters {
        string(name: 'TESTS', defaultValue: 'integration,statelest,stateful,unit', description: 'Tests 测试项标识符')

        string(name: 'TESTS_TAG', defaultValue: "${env.BUILD_TAG.replaceAll('%2F', '-')}", description: '测试镜像TAG e.g. latest env.BUILD_TAG 100')

        booleanParam(name: 'CLEAN_WS', defaultValue: false, description: 'Will clear the workspace, and clone the entire daisy repository.')

        booleanParam(name: 'REBUILD_DOCKER_IMAGE', defaultValue: false, description: "Will rebuild all docker image used by daisy.")

        booleanParam(name: 'ALL_TEST', defaultValue: false, description: 'Run all test')
        
        booleanParam(name: 'SANITIZER', defaultValue: false, description: 'Run sanitizer')

        booleanParam(name: 'COVERAGE', defaultValue: false, description: 'Generate code coverage report')

        booleanParam(name: 'RELEASE', defaultValue: false, description: 'Generate release package')

        text(name: 'TAG', defaultValue: '', description: "Enter the tag of this build, default value is {BRANCH_NAME}-{GIT_COMMIT}")
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
        stage('Build ASAN Binary') {
            agent { label 'builder'}
            steps {
                fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                copyCredentialFile("sccache_config", "sccache_dir")
                
                dir("docker/packager") {
                    sh "./packager --package-type binary --cache=sccache --compiler=clang-12 --docker-image-repo=registry.foundary.zone:8360  --docker-image-version=sccache-clang-12 --output-dir ${env.WORKSPACE}/output --sccache_dir=${env.WORKSPACE}/sccache_dir --sanitizer=address"
                }

                sh "tar -zcvf clickhouse-ASAN-${params.TESTS_TAG}.tar.gz output/*"
                archiveArtifacts artifacts: "clickhouse-ASAN-${params.TESTS_TAG}.tar.gz", followSymlinks: false
            }
        }
        stage ('Build Tests Image') {
            agent { label 'ph' }
            steps {
                // echo "skip"
                fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                dir ('tests/tests-env') {
                    script {
                        sh 'mkdir -p clickhouse-tests-env && rm -rf clickhouse-tests-env/*'
                        copyArtifacts filter: "clickhouse-*-${params.TESTS_TAG}.tar.gz", fingerprintArtifacts: true, projectName: env.JOB_NAME, selector: specific(env.BUILD_NUMBER), target: '.'
                        sh "for i in clickhouse-*-${params.TESTS_TAG}.tar.gz; do bin_path=clickhouse-tests-env/`echo \$i | sed -r \"s/clickhouse-(.*)-${params.TESTS_TAG}\\.tar\\.gz/\\1/g\"`-bin; mkdir -p \$bin_path; tar -zxvf \$i -C .; mv output/* \$bin_path/ >/dev/null 2>&1; cp ../clickhouse-test \$bin_path ; done"
                        sh "rsync -aL ../config clickhouse-tests-env/ && cp ../../programs/server/config.xml ../users.xml clickhouse-tests-env/config/ && rsync -aL ../queries clickhouse-tests-env/ && rsync -aL ../integration clickhouse-tests-env/"
                        sh """cat > Dockerfile << EOF
                            FROM ubuntu:20.04
                            COPY clickhouse-tests-env /clickhouse-tests-env
                            """
                    }
                }
                script {
                    docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                        docker.build("daisy/clickhouse-tests-env:${params.TESTS_TAG}", "${WORKSPACE}/tests/tests-env/").push()
                        docker.build("daisy/clickhouse-integration-tests-runner:${params.TESTS_TAG}", "${WORKSPACE}/docker/test/integration/daisy_runner/").push()
                        docker.build("daisy/clickhouse-statelest-tests-runner:${params.TESTS_TAG}", "${WORKSPACE}/docker/test/stateless_unbundled/daisy_runner/").push()
                        docker.build("daisy/clickhouse-stateful-tests-runner:${params.TESTS_TAG}", "--build-arg TAG=${params.TESTS_TAG} ${WORKSPACE}/docker/test/stateful/daisy_runner/").push()
                        docker.build("daisy/clickhouse-unit-tests-runner:${params.TESTS_TAG}", "${WORKSPACE}/docker/test/unit/daisy_runner/").push()
                    }
                }
            }
        }
        stage ('Parallel Cases') {
            failFast false
            parallel {
                stage ('Case-1: Tests On ASan') {
                    agent { label 'ph' }
                    steps {
                        script {
                            docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                                def tests = [:]
                                for (id in params.TESTS.tokenize(',')) {
                                    tests.put("Test-" + id + " On ASan", Base_Tests("ASAN", id))
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

                // stage ('Case-2: Tests On TSan') {
                //     steps {
                //         script {
                //             docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                //                 def tests = [:]
                //                 for (id in params.TESTS.tokenize(',')) {
                //                     tests.put("Test-" + id + " On TSan", Base_Tests("TSAN", id))
                //                 }
                //                 parallel tests
                //             }
                //         }
                //     }
                //     post {
                //         always {
                //             archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                //             sh "rm -rf reports/*.*"
                //             script {
                //                 ExistedContainers.each { it.stop() }  /// 该操作等价于'docker stop && docker rm'
                //             }
                //         }
                //     }
                // }

                // stage ('Case-3: Tests On MSan') {
                //     steps {
                //         script {
                //             docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                //                 def tests = [:]
                //                 for (id in params.TESTS.tokenize(',')) {
                //                     tests.put("Test-" + id + " On MSan", Base_Tests("MSAN", id))
                //                 }
                //                 parallel tests
                //             }
                //         }
                //     }
                //     post {
                //         always {
                //             archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                //             sh "rm -rf reports/*.*"
                //             script {
                //                 ExistedContainers.each { it.stop() }  /// 该操作等价于'docker stop && docker rm'
                //             }
                //         }
                //     }
                // }

                // stage ('Case-4: Tests On USan') {
                //     steps {
                //         script {
                //             docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                //                 def tests = [:]
                //                 for (id in params.TESTS.tokenize(',')) {
                //                     tests.put("Test-" + id + " On USan", Base_Tests("USAN", id))
                //                 }
                //                 parallel tests
                //             }
                //         }
                //     }
                //     post {
                //         always {
                //             archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                //             sh "rm -rf reports/*.*"
                //             script {
                //                 ExistedContainers.each { it.stop() }  /// 该操作等价于'docker stop && docker rm'
                //             }
                //         }
                //     }
                // }

                // stage ('Case-5: Tests On Coverage') {
                //     steps {
                //         script {
                //             docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                //                 def tests = [:]
                //                 for (id in params.TESTS.tokenize(',')) {
                //                     tests.put("Test-" + id + " On Coverage", Base_Tests("COVERAGE", id))
                //                 }
                //                 parallel tests
                //             }
                //         }
                //     }
                //     post {
                //         always {
                //             archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                //             sh "rm -rf reports/*.*"
                //             script {
                //                 ExistedContainers.each { it.stop() }  /// 该操作等价于'docker stop && docker rm'
                //             }
                //         }
                //     }
                // }
            }
        }
    }
}