node {
    stage ('LinkbenchTest'){
        // checkout scm
        git url: 'https://github.com/IlyaMarkovMipt/linkbench.git'
        withCredentials([file(credentialsId: '972063c2-e66b-4cd8-bfe0-93de36c20cd6', variable: 'FILE')]){
            sh "cat $FILE > auth.conf"
            def env = docker.build "linkbench"
            env.inside{
                    sh 'tarantool app.lua'
                    sh 'sh /linkbench/src/tarantool_scripts/linkbench_client.sh'
            }
        }
    }
}
