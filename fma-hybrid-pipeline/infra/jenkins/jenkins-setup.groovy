// Jenkins initial configuration script
// This script runs on Jenkins startup to configure credentials and basic settings

import jenkins.model.*
import com.cloudbees.plugins.credentials.*
import com.cloudbees.plugins.credentials.common.*
import com.cloudbees.plugins.credentials.domains.*
import com.cloudbees.plugins.credentials.impl.*
import org.jenkinsci.plugins.plaincredentials.impl.*
import hudson.util.Secret

// Get Jenkins instance
def jenkins = Jenkins.getInstance()

// Get credentials store
def domain = Domain.global()
def store = jenkins.getExtensionList('com.cloudbees.plugins.credentials.SystemCredentialsProvider')[0].getStore()

// Define credentials from environment variables
def credentialsList = [
    // AWS Credentials
    [
        id: 'aws-credentials-id',
        description: 'AWS Access Credentials for S3',
        username: System.getenv('AWS_ACCESS_KEY_ID') ?: 'your-aws-access-key',
        password: System.getenv('AWS_SECRET_ACCESS_KEY') ?: 'your-aws-secret-key'
    ],
    
    // Databricks Host
    [
        id: 'databricks-host',
        description: 'Databricks Workspace URL',
        secret: System.getenv('DATABRICKS_HOST') ?: 'https://your-workspace.cloud.databricks.com'
    ],
    
    // Databricks Token
    [
        id: 'databricks-token',
        description: 'Databricks Access Token',
        secret: System.getenv('DATABRICKS_TOKEN') ?: 'your-databricks-token'
    ],
    
    // Databricks Cluster ID
    [
        id: 'databricks-cluster-id',
        description: 'Databricks Cluster ID',
        secret: System.getenv('DATABRICKS_CLUSTER_ID') ?: 'your-cluster-id'
    ],
    
    // MLflow Tracking URI
    [
        id: 'mlflow-tracking-uri',
        description: 'MLflow Tracking Server URI',
        secret: System.getenv('MLFLOW_TRACKING_URI') ?: 'databricks'
    ]
]

// Create credentials
credentialsList.each { cred ->
    if (cred.containsKey('username')) {
        // Username/Password credentials
        def credentials = new UsernamePasswordCredentialsImpl(
            CredentialsScope.GLOBAL,
            cred.id,
            cred.description,
            cred.username,
            cred.password
        )
        store.addCredentials(domain, credentials)
        println "Created username/password credential: ${cred.id}"
    } else {
        // Secret text credentials
        def credentials = new StringCredentialsImpl(
            CredentialsScope.GLOBAL,
            cred.id,
            cred.description,
            Secret.fromString(cred.secret)
        )
        store.addCredentials(domain, credentials)
        println "Created secret credential: ${cred.id}"
    }
}

// Configure Jenkins settings
jenkins.setNumExecutors(2)
jenkins.setScmCheckoutRetryCount(3)

// Save configuration
jenkins.save()

println "Jenkins configuration completed successfully!"
