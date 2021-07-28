@Library('shared-lib') _

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
            'image' : docker.image("daisy/clickhouse-integration-tests-runner"),
            'name' : "${utag}_daisy_integration_tests_runner",
            'args' : "--privileged -e TEST_TAG=${utag} -e LLVM_PROFILE_FILE=/tests_output/coverage_reports/integration_tests_server_%h_%p_%m.profraw -v /var/lib/docker",
            'loaders' :
            [
                'env' :
                [
                    'volume' : "${WORKSPACE}/clickhouse-tests-env:/clickhouse-tests-env"
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [
                "ln -sf /clickhouse-tests-env/bin/* /usr/bin/",
                '/clickhouse-tests-env/config/install.sh',
                'ln -sf /clickhouse-tests-env/config/config.xml /etc/clickhouse-server/',
                'ln -sf /clickhouse-tests-env/config/users.xml /etc/clickhouse-server/',
                "dockerd-entrypoint.sh sh -c \"pytest --html=/tests_output/${tag}_IntegrationTest.html --self-contained-html \""
            ]
        ],
        'statelest' :
        [
            'stage_name' : "${tag}-Test-Statelest: statelest tests in docker",
            'image' : docker.image("daisy/clickhouse-statelest-tests-runner"),
            'name' : "${utag}_daisy_statelest_tests_runner",
            'args' : "--net=none -e TEST_TAG=${tag}",
            'loaders' :
            [
                'env' :
                [
                    'volume' : "${WORKSPACE}/clickhouse-tests-env:/clickhouse-tests-env"
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [ 
                "ln -sf /clickhouse-tests-env/bin/* /usr/bin/",
                '/clickhouse-tests-env/config/install.sh',
                'ln -sf /clickhouse-tests-env/config/config.xml /etc/clickhouse-server/',
                'ln -sf /clickhouse-tests-env/config/users.xml /etc/clickhouse-server/',
                'dockerd-entrypoint.sh -j2'
            ]
        ],
        'stateful' :
        [
            'stage_name' : "${tag}-Test-Stateful: stateful tests in docker",
            'image' : docker.image("daisy/clickhouse-stateful-tests-runner"),
            'name' : "${utag}_daisy_stateful_tests_runner",
            'args' : "--net=none -e TEST_TAG=${tag}",
            'loaders' :
            [
                'env' :
                [
                    'volume' : "${WORKSPACE}/clickhouse-tests-env:/clickhouse-tests-env"
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [ 
                "ln -sf /clickhouse-tests-env/bin/* /usr/bin/",
                '/clickhouse-tests-env/config/install.sh',
                'ln -sf /clickhouse-tests-env/config/config.xml /etc/clickhouse-server/',
                'ln -sf /clickhouse-tests-env/config/users.xml /etc/clickhouse-server/',
                'rm /etc/clickhouse-server/config.d/listen.xml /etc/clickhouse-server/config.d/zookeeper.xml /etc/clickhouse-server/config.d/keeper_port.xml',
                'dockerd-entrypoint.sh -j2'
            ]
        ],
        'unit' :
        [
            'stage_name' : "${tag}-Test-Unit: unit tests in docker",
            'image' : docker.image("daisy/clickhouse-unit-tests-runner"),
            'name' : "${utag}_daisy_unit_tests_runner",
            'args' : "--net=none -e TEST_TAG=${tag}",
            'loaders' :
            [
                'env' :
                [
                    'volume' : "${WORKSPACE}/clickhouse-tests-env:/clickhouse-tests-env"
                ]
            ],
            'reports' : "/tests_output",
            'inside_cmd' :
            [ 
                "ln -sf /clickhouse-tests-env/bin/* /usr/bin/",
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
            def ExisitedContainers = []
            try {
                def f =
                {
                    if (!loaders.isEmpty())
                    {  /// 前置加载
                        def loader = loaders.pop()
                        if (loader.image)
                        {   /// 挂载镜像容器卷
                            assert loader.name && loader.volume
                            loader.image.pull()
                            sh "docker tag registry.foundary.zone:8360/${loader.image.id} ${loader.image.id}"
                            def c = loader.image.run(" -u root -it --entrypoint='' --name ${loader.name} -v ${loader.volume}", 'cat')
                            ExisitedContainers?.push(c)
                            loader.inside_cmd?.each { item -> sh "docker exec ${c.id} sh -c \'${item}\'" }
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
                        runner.image.pull()
                        sh "docker tag registry.foundary.zone:8360/${runner.image.id} ${runner.image.id}"
                        def c = runner.image.run(runner.args + " -u root -it --entrypoint='' --name ${runner.name} ", 'cat')
                        ExisitedContainers?.push(c)
                        runner.inside_cmd?.each { item -> sh "docker exec ${c.id} sh -c \'${item}\'" }
                    }
                }()
            } catch(err) {
                echo "Caught: ${err}"
                unstable('Tests failed!')
            } finally {
                ExisitedContainers.each { c-> sh "docker rm -vf ${c.id} || true" }
            }
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

        string(name: 'USE_BINARY_NUMBER', defaultValue: "$env.BUILD_NUMBER", description: "use specified number of binary pkg. default: now build number")

        booleanParam(name: 'CLEAN_WS', defaultValue: true, description: 'After build, will clear the workspace')

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
                script {
                    if(params.CLEAN_WS) {
                        sh "sudo chown -R \$(whoami) ${WORKSPACE}"  // set the `$(whoami)` user sudoer only for chown by nopasswd
                        cleanWs(deleteDirs: true, disableDeferredWipeout: true)
                    }
                }
                checkout scm
                archiveSource()
            }
        }

        stage('Build Docker Image') {
            agent {
                node {
                    label 'bj'
                    customWorkspace "${env.WORKSPACE}/CICD_build_image"
                }
            }
            steps {
                script {
                    if(params.REBUILD_DOCKER_IMAGE) {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        sh "python3 utils/ci/build_images.py"
                        // TODO: docker push

                        docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                            docker.build("daisy/clickhouse-integration-tests-runner", "${WORKSPACE}/docker/test/integration/daisy_runner/").push()
                            docker.build("daisy/clickhouse-statelest-tests-runner", "${WORKSPACE}/docker/test/stateless_unbundled/daisy_runner/").push()
                            docker.build("daisy/clickhouse-stateful-tests-runner", "${WORKSPACE}/docker/test/stateful/daisy_runner/").push()
                            docker.build("daisy/clickhouse-unit-tests-runner", "${WORKSPACE}/docker/test/unit/daisy_runner/").push()
                        }
                    } else {
                        echo "Skip build docker image"
                    }
                }
            }
        }

        stage('Build Clang Tidy Binary') {
            agent { 
                node {
                    label 'builder'
                    customWorkspace "${env.WORKSPACE}/CICD_build_clang"
                }
            }
            steps {
                script {
                    if(params.USE_BINARY_NUMBER != env.BUILD_NUMBER) {
                        echo "reuse old number \'${params.USE_BINARY_NUMBER}\', so skip"
                    } else {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        copyCredentialFile("sccache_config", "sccache_dir")
                        
                        dir("docker/packager") {
                            sh "./packager --package-type binary --cache=sccache --compiler=clang-12 --docker-image-repo=registry.foundary.zone:8360  --docker-image-version=sccache-clang-12 --output-dir ${env.WORKSPACE}/output --sccache_dir=${env.WORKSPACE}/sccache_dir --clang-tidy"
                        }
                    }
                }
            }
        }

        stage('Build Coverage Binary') {
            agent { 
                node {
                    label 'builder'
                    customWorkspace "${env.WORKSPACE}/CICD_build_coverage"
                }
            }
            steps {
                script {
                    if(params.USE_BINARY_NUMBER != env.BUILD_NUMBER) {
                        echo "reuse old number \'${params.USE_BINARY_NUMBER}\', so skip"
                    } else {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        copyCredentialFile("sccache_config", "sccache_dir")
                        
                        dir("docker/packager") {
                            sh "./packager --package-type binary --cache=sccache --compiler=clang-12 --docker-image-repo=registry.foundary.zone:8360  --docker-image-version=sccache-clang-12 --output-dir ${env.WORKSPACE}/output --sccache_dir=${env.WORKSPACE}/sccache_dir --with-coverage"
                        }
                        // package binary & tests
                        sh 'mkdir -p clickhouse-tests-env && rm -rf clickhouse-tests-env/*'
                        sh "bin_path=clickhouse-tests-env/bin; mkdir -p \$bin_path; cp -ar output/* \$bin_path/; cp tests/clickhouse-test \$bin_path"
                        sh "rsync -aL tests/config clickhouse-tests-env/ && cp programs/server/config.xml tests/users.xml clickhouse-tests-env/config/ && rsync -aL tests/queries clickhouse-tests-env/ && rsync -aL tests/integration clickhouse-tests-env/"    
                        sh "tar -zcvf clickhouse-COVERAGE-${params.TESTS_TAG}.tar.gz clickhouse-tests-env"
                        archiveArtifacts artifacts: "clickhouse-COVERAGE-${params.TESTS_TAG}.tar.gz", followSymlinks: false
                    }
                }
            }
        }

        stage('Build ASAN Binary') {
            agent { 
                node {
                    label 'builder'
                    customWorkspace "${env.WORKSPACE}/CICD_build_asan"
                }
            }
            steps {
                script {
                    if(params.USE_BINARY_NUMBER != env.BUILD_NUMBER) {
                        echo "reuse old number \'${params.USE_BINARY_NUMBER}\', so skip"
                    } else {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        copyCredentialFile("sccache_config", "sccache_dir")
                        
                        dir("docker/packager") {
                            sh "./packager --package-type binary --cache=sccache --compiler=clang-12 --docker-image-repo=registry.foundary.zone:8360  --docker-image-version=sccache-clang-12 --output-dir ${env.WORKSPACE}/output --sccache_dir=${env.WORKSPACE}/sccache_dir --sanitizer=address"
                        }
                        // package binary & tests
                        sh 'mkdir -p clickhouse-tests-env && rm -rf clickhouse-tests-env/*'
                        sh "bin_path=clickhouse-tests-env/bin; mkdir -p \$bin_path; cp -ar output/* \$bin_path/; cp tests/clickhouse-test \$bin_path"
                        sh "rsync -aL tests/config clickhouse-tests-env/ && cp programs/server/config.xml tests/users.xml clickhouse-tests-env/config/ && rsync -aL tests/queries clickhouse-tests-env/ && rsync -aL tests/integration clickhouse-tests-env/"    
                        sh "tar -zcvf clickhouse-ASAN-${params.TESTS_TAG}.tar.gz clickhouse-tests-env"
                        archiveArtifacts artifacts: "clickhouse-ASAN-${params.TESTS_TAG}.tar.gz", followSymlinks: false
                    }
                }
            }
        }

        stage('Build MSAN Binary') {
            agent { 
                node {
                    label 'builder'
                    customWorkspace "${env.WORKSPACE}/CICD_build_msan"
                }
            }
            steps {
                script {
                    if(params.USE_BINARY_NUMBER != env.BUILD_NUMBER) {
                        echo "reuse old number \'${params.USE_BINARY_NUMBER}\', so skip"
                    } else {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        copyCredentialFile("sccache_config", "sccache_dir")
                        
                        dir("docker/packager") {
                            sh "./packager --package-type binary --cache=sccache --compiler=clang-12 --docker-image-repo=registry.foundary.zone:8360  --docker-image-version=sccache-clang-12 --output-dir ${env.WORKSPACE}/output --sccache_dir=${env.WORKSPACE}/sccache_dir --sanitizer=memory"
                        }
                        // package binary & tests
                        sh 'mkdir -p clickhouse-tests-env && rm -rf clickhouse-tests-env/*'
                        sh "bin_path=clickhouse-tests-env/bin; mkdir -p \$bin_path; cp -ar output/* \$bin_path/; cp tests/clickhouse-test \$bin_path"
                        sh "rsync -aL tests/config clickhouse-tests-env/ && cp programs/server/config.xml tests/users.xml clickhouse-tests-env/config/ && rsync -aL tests/queries clickhouse-tests-env/ && rsync -aL tests/integration clickhouse-tests-env/"    
                        sh "tar -zcvf clickhouse-MSAN-${params.TESTS_TAG}.tar.gz clickhouse-tests-env"
                        archiveArtifacts artifacts: "clickhouse-MSAN-${params.TESTS_TAG}.tar.gz", followSymlinks: false
                    }
                }
            }
        }

        stage('Build TSAN Binary') {
            agent { 
                node {
                    label 'builder'
                    customWorkspace "${env.WORKSPACE}/CICD_build_tsan"
                }
            }

            steps {
                script {
                    if(params.USE_BINARY_NUMBER != env.BUILD_NUMBER) {
                        echo "reuse old number \'${params.USE_BINARY_NUMBER}\', so skip"
                    } else {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        copyCredentialFile("sccache_config", "sccache_dir")
                        
                        dir("docker/packager") {
                            sh "./packager --package-type binary --cache=sccache --compiler=clang-12 --docker-image-repo=registry.foundary.zone:8360  --docker-image-version=sccache-clang-12 --output-dir ${env.WORKSPACE}/output --sccache_dir=${env.WORKSPACE}/sccache_dir --sanitizer=thread"
                        }
                        // package binary & tests
                        sh 'mkdir -p clickhouse-tests-env && rm -rf clickhouse-tests-env/*'
                        sh "bin_path=clickhouse-tests-env/bin; mkdir -p \$bin_path; cp -ar output/* \$bin_path/; cp tests/clickhouse-test \$bin_path"
                        sh "rsync -aL tests/config clickhouse-tests-env/ && cp programs/server/config.xml tests/users.xml clickhouse-tests-env/config/ && rsync -aL tests/queries clickhouse-tests-env/ && rsync -aL tests/integration clickhouse-tests-env/"    
                        sh "tar -zcvf clickhouse-TSAN-${params.TESTS_TAG}.tar.gz clickhouse-tests-env"
                        archiveArtifacts artifacts: "clickhouse-TSAN-${params.TESTS_TAG}.tar.gz", followSymlinks: false
                    }
                }
            }
        }

        stage('Build USAN Binary') {
            agent { 
                node {
                    label 'builder'
                    customWorkspace "${env.WORKSPACE}/CICD_build_usan"
                }
            }
            
            steps {
                script {
                    if(params.USE_BINARY_NUMBER != env.BUILD_NUMBER) {
                        echo "reuse old number \'${params.USE_BINARY_NUMBER}\', so skip"
                    } else {
                        fetchSource(env.JOB_NAME, env.BUILD_NUMBER)
                        copyCredentialFile("sccache_config", "sccache_dir")
                        
                        dir("docker/packager") {
                            sh "./packager --package-type binary --cache=sccache --compiler=clang-12 --docker-image-repo=registry.foundary.zone:8360  --docker-image-version=sccache-clang-12 --output-dir ${env.WORKSPACE}/output --sccache_dir=${env.WORKSPACE}/sccache_dir --sanitizer=undefined"
                        }
                        // package binary & tests
                        sh 'mkdir -p clickhouse-tests-env && rm -rf clickhouse-tests-env/*'
                        sh "bin_path=clickhouse-tests-env/bin; mkdir -p \$bin_path; cp -ar output/* \$bin_path/; cp tests/clickhouse-test \$bin_path"
                        sh "rsync -aL tests/config clickhouse-tests-env/ && cp programs/server/config.xml tests/users.xml clickhouse-tests-env/config/ && rsync -aL tests/queries clickhouse-tests-env/ && rsync -aL tests/integration clickhouse-tests-env/"    
                        sh "tar -zcvf clickhouse-USAN-${params.TESTS_TAG}.tar.gz clickhouse-tests-env"
                        archiveArtifacts artifacts: "clickhouse-USAN-${params.TESTS_TAG}.tar.gz", followSymlinks: false
                    }
                }
            }
        }

        stage ('Parallel Cases') {
            parallel {
                stage ('Case-1: Tests On ASan') {
                    agent { 
                        node {
                            label 'test1'
                            customWorkspace "${env.WORKSPACE}/CICD_Tests_On_ASan"
                        }
                    }
                    steps {
                        script {
                            cleanWs()
                            copyArtifacts filter: "clickhouse-ASAN-${params.TESTS_TAG}.tar.gz", fingerprintArtifacts: true, projectName: env.JOB_NAME, selector: specific(params.USE_BINARY_NUMBER), target: '.'
                            sh "tar -zxvf clickhouse-ASAN-${params.TESTS_TAG}.tar.gz -C . >/dev/null 2>&1 && rm -rf clickhouse-ASAN-${params.TESTS_TAG}.tar.gz"

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
                            script {
                                if(params.CLEAN_WS) {
                                    sh "sudo chown -R \$(whoami) ${WORKSPACE}"  // set the `$(whoami)` user sudoer only for chown by nopasswd
                                    cleanWs(deleteDirs: true, disableDeferredWipeout: true)
                                }
                            }
                        }
                    }
                }

                stage ('Case-2: Tests On TSan') {
                    agent { 
                        node {
                            label 'test2'
                            customWorkspace "${env.WORKSPACE}/CICD_Tests_On_TSan"
                        }
                    }
                    steps {
                        script {
                            cleanWs()
                            copyArtifacts filter: "clickhouse-TSAN-${params.TESTS_TAG}.tar.gz", fingerprintArtifacts: true, projectName: env.JOB_NAME, selector: specific(params.USE_BINARY_NUMBER), target: '.'
                            sh "tar -zxvf clickhouse-TSAN-${params.TESTS_TAG}.tar.gz -C . >/dev/null 2>&1 && rm -rf clickhouse-TSAN-${params.TESTS_TAG}.tar.gz"

                            docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                                def tests = [:]
                                for (id in params.TESTS.tokenize(',')) {
                                    tests.put("Test-" + id + " On TSan", Base_Tests("TSAN", id))
                                }
                                parallel tests
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                            script {
                                if(params.CLEAN_WS) {
                                    sh "sudo chown -R \$(whoami) ${WORKSPACE}"  // set the `$(whoami)` user sudoer only for chown by nopasswd
                                    cleanWs(deleteDirs: true, disableDeferredWipeout: true)
                                }
                            }
                        }
                    }
                }

                stage ('Case-3: Tests On MSan') {
                    agent { 
                        node {
                            label 'test1'
                            customWorkspace "${env.WORKSPACE}/CICD_Tests_On_MSan"
                        }
                    }
                    steps {
                        script {
                            cleanWs()
                            copyArtifacts filter: "clickhouse-MSAN-${params.TESTS_TAG}.tar.gz", fingerprintArtifacts: true, projectName: env.JOB_NAME, selector: specific(params.USE_BINARY_NUMBER), target: '.'
                            sh "tar -zxvf clickhouse-MSAN-${params.TESTS_TAG}.tar.gz -C . >/dev/null 2>&1 && rm -rf clickhouse-MSAN-${params.TESTS_TAG}.tar.gz"

                            docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                                def tests = [:]
                                for (id in params.TESTS.tokenize(',')) {
                                    tests.put("Test-" + id + " On MSan", Base_Tests("MSAN", id))
                                }
                                parallel tests
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                            script {
                                if(params.CLEAN_WS) {
                                    sh "sudo chown -R \$(whoami) ${WORKSPACE}"  // set the `$(whoami)` user sudoer only for chown by nopasswd
                                    cleanWs(deleteDirs: true, disableDeferredWipeout: true)
                                }
                            }
                        }
                    }
                }

                stage ('Case-4: Tests On USan') {
                    agent { 
                        node {
                            label 'test2'
                            customWorkspace "${env.WORKSPACE}/CICD_Tests_On_USan"
                        }
                    }
                    steps {
                        script {
                            cleanWs()
                            copyArtifacts filter: "clickhouse-USAN-${params.TESTS_TAG}.tar.gz", fingerprintArtifacts: true, projectName: env.JOB_NAME, selector: specific(params.USE_BINARY_NUMBER), target: '.'
                            sh "tar -zxvf clickhouse-USAN-${params.TESTS_TAG}.tar.gz -C . >/dev/null 2>&1 && rm -rf clickhouse-USAN-${params.TESTS_TAG}.tar.gz"

                            docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                                def tests = [:]
                                for (id in params.TESTS.tokenize(',')) {
                                    tests.put("Test-" + id + " On USan", Base_Tests("USAN", id))
                                }
                                parallel tests
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                            script {
                                if(params.CLEAN_WS) {
                                    sh "sudo chown -R \$(whoami) ${WORKSPACE}"  // set the `$(whoami)` user sudoer only for chown by nopasswd
                                    cleanWs(deleteDirs: true, disableDeferredWipeout: true)
                                }
                            }
                        }
                    }
                }

                stage ('Case-5: Tests On Coverage') {
                    agent { 
                        node {
                            label 'test1'
                            customWorkspace "${env.WORKSPACE}/CICD_Tests_On_Coverage"
                        }
                    }
                    environment {
                        COVERAGE_DIR="reports/coverage_reports"
                    }
                    stages {
                        stage ('5.1 Test') {
                            steps {
                                script {
                                    cleanWs()
                                    copyArtifacts filter: "clickhouse-COVERAGE-${params.TESTS_TAG}.tar.gz", fingerprintArtifacts: true, projectName: env.JOB_NAME, selector: specific(params.USE_BINARY_NUMBER), target: '.'
                                    sh "tar -zxvf clickhouse-COVERAGE-${params.TESTS_TAG}.tar.gz -C . >/dev/null 2>&1 && rm -rf clickhouse-COVERAGE-${params.TESTS_TAG}.tar.gz"
                                    sh "mkdir -p ${COVERAGE_DIR}"
                                    docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                                        def tests = [:]
                                        for (id in params.TESTS.tokenize(',')) {
                                            tests.put("Test-" + id + " On Coverage", Base_Tests("COVERAGE", id))
                                        }
                                        parallel tests
                                    }
                                }
                            }
                        }
                        stage ('5.2 Coverage') {
                            steps {
                                script {
                                    docker.withRegistry('http://registry.foundary.zone:8360', 'cicd-dockerhub-id') {
                                        docker.image('yandex/clickhouse-binary-builder:sccache-clang-12').inside("-u root -v ${WORKSPACE}/${COVERAGE_DIR}:/${COVERAGE_DIR} -v ${WORKSPACE}/clickhouse-tests-env/bin/clickhouse:/clickhouse") {
                                            sh "apt-get update && apt-get install -y lcov"
                                            sh "llvm-profdata-12 merge -sparse ${COVERAGE_DIR}/*.profraw --failure-mode=all -o ${COVERAGE_DIR}/clickhouse.profdata"
                                            sh "llvm-cov-12 export /clickhouse -instr-profile=${COVERAGE_DIR}/clickhouse.profdata -j=8 -format=lcov -ignore-filename-regex '.*contrib.*' > ${COVERAGE_DIR}/output.lcov"
                                            sh "genhtml ${COVERAGE_DIR}/output.lcov --ignore-errors source --output-directory ${COVERAGE_DIR}/html/"
                                            sh "cp ${COVERAGE_DIR}/html/index.html reports/COVERAGE_index.html && tar -czvf reports/COVERAGE_reports.tar.gz ${COVERAGE_DIR}/html/*"
                                        }
                                    }
                                }
                            }
                        }
                    }
                    post {
                        always {
                            archiveArtifacts allowEmptyArchive: true, artifacts: "reports/*.*", followSymlinks: false
                            script {
                                if(params.CLEAN_WS) {
                                    sh "sudo chown -R \$(whoami) ${WORKSPACE}"  // set the `$(whoami)` user sudoer only for chown by nopasswd
                                    cleanWs(deleteDirs: true, disableDeferredWipeout: true)
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
