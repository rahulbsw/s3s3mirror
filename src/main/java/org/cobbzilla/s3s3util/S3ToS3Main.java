package org.cobbzilla.s3s3util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.CmdLineParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Provides the "main" method. Responsible for parsing options and setting up the MirrorMaster to manage the copy.
 */
@Slf4j
public class S3ToS3Main {

    @Getter @Setter private String[] args;

    @Getter private final S3ToS3Options options = new S3ToS3Options();

    private final CmdLineParser parser = new CmdLineParser(options);

    private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) -> log.error("Uncaught Exception (thread "+t.getName()+"): "+e, e);

    @Getter private AmazonS3 client;
    @Getter private S3ToS3Context context;
    @Getter private S3ToS3Master master;

    public S3ToS3Main(String[] args) { this.args = args; }

    public static void main (String[] args) {
        S3ToS3Main main = new S3ToS3Main(args);
        main.run();
    }

    public void run() {
        init();
        master.mirror();
    }

    public void init() {
        if (client == null) {
            try {
                parseArguments();
            } catch (Exception e) {
                System.err.println(e.getMessage());
                parser.printUsage(System.err);
                System.exit(1);
            }

            client = getAmazonS3Client();
            context = new S3ToS3Context(options);
            master = new S3ToS3Master(client, context);

            Runtime.getRuntime().addShutdownHook(context.getStats().getShutdownHook());
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

    protected AmazonS3 getAmazonS3Client() {
        ClientConfiguration clientConfiguration = new ClientConfiguration().withProtocol((options.isSsl() ? Protocol.HTTPS : Protocol.HTTP))
                .withMaxConnections(options.getMaxConnections());
        if (options.getHasProxy()) {
            clientConfiguration = clientConfiguration
                    .withProxyHost(options.getProxyHost())
                    .withProxyPort(options.getProxyPort());
        }
        AmazonS3 client;
        if (options.hasAwsKeys()) {
            client=AmazonS3Client.builder().withCredentials(new EnvironmentVariableCredentialsProvider()).withClientConfiguration(clientConfiguration).build();
        } else if (options.isUseIamRole()) {
            client = AmazonS3Client.builder().withCredentials(new InstanceProfileCredentialsProvider(true)).withClientConfiguration(clientConfiguration).build();
        } else {
            throw new IllegalStateException("No authenication method available, please specify IAM Role usage or AWS key and secret");
        }        
        if (options.hasEndpoint()) client.setEndpoint(options.getEndpoint());
        return client;
    }

    protected void parseArguments() throws Exception {
        parser.parseArgument(args);
        
        // for credentials, check for IAM role usage if not then...
        // try the .aws/config file first if there is a profile specified, otherwise defer to
        // .s3cfg before using the default .aws/config credentials 
        // (this may attempt .aws/config twice for no reason, but maintains backward compatibility)
        if (!options.isUseIamRole()) {
            if (!options.hasAwsKeys() && options.getProfile() != null) loadAwsKeysFromAwsConfig();
            if (!options.hasAwsKeys()) loadAwsKeysFromS3Config();
            if (!options.hasAwsKeys()) loadAwsKeysFromAwsConfig();
            if (!options.hasAwsKeys()) loadAwsKeysFromAwsCredentials();
            if (!options.hasAwsKeys()) {
                throw new IllegalStateException("Could not find credentials, IAM Role usage not specified and ENV vars not defined: " + S3ToS3Options.AWS_ACCESS_KEY + " and/or " + S3ToS3Options.AWS_SECRET_KEY);
            }
        } else {
            InstanceProfileCredentialsProvider client = new InstanceProfileCredentialsProvider(true);
            if (client.getCredentials() == null) {
                throw new IllegalStateException("Could not find IAM Instance Profile credentials from the AWS metadata service.");
            }
        }
        options.initDerivedFields();
    }

    private void loadAwsKeysFromS3Config() {
        try {
            // try to load from ~/.s3cfg
            @Cleanup BufferedReader reader = new BufferedReader(new FileReader(System.getProperty("user.home")+File.separator+".s3cfg"));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().startsWith("access_key")) {
                    options.setAWSAccessKeyId(line.substring(line.indexOf("=") + 1).trim());
                } else if (line.trim().startsWith("secret_key")) {
                    options.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
                } else if (!options.getHasProxy() && line.trim().startsWith("proxy_host")) {
                    options.setProxyHost(line.substring(line.indexOf("=") + 1).trim());
                } else if (!options.getHasProxy() && line.trim().startsWith("proxy_port")){
                    options.setProxyPort(Integer.parseInt(line.substring(line.indexOf("=") + 1).trim()));
                }
            }
        } catch (Exception e) {
            // ignore - let other credential-discovery processes have a crack
        }
    }

    private void loadAwsKeysFromAwsConfig() {
        try {
            // try to load from ~/.aws/config
            @Cleanup BufferedReader reader = new BufferedReader(new FileReader(
                    System.getProperty("user.home") + File.separator + ".aws" + File.separator + "config"));
            String line;
            boolean skipSection = true;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("[")) {
                    // if no defined profile, use '[default]' otherwise use profile with matching name
                    skipSection = (options.getProfile() != null || !line.equals("[default]"))
                            && (options.getProfile() == null || !line.equals("[profile " + options.getProfile() + "]"));
                    continue;
                }
                if (skipSection) continue;
                if (line.startsWith("aws_access_key_id")) {
                    options.setAWSAccessKeyId(line.substring(line.indexOf("=") + 1).trim());
                } else if (line.startsWith("aws_secret_access_key")) {
                    options.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
                }
            }
        } catch (Exception e) {
            // ignore - let other credential-discovery processes have a crack
        }
    }
    
    private void loadAwsKeysFromAwsCredentials() {
        try {
            // try to load from ~/.aws/config
            @Cleanup BufferedReader reader = new BufferedReader(new FileReader(
                    System.getProperty("user.home") + File.separator + ".aws" + File.separator + "credentials"));
            String line;
            boolean skipSection = true;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.startsWith("[")) {
                    // if no defined profile, use '[default]' otherwise use profile with matching name
                    skipSection = (options.getProfile() != null || !line.equals("[default]"))
                            && (options.getProfile() == null || !line.equals("[" + options.getProfile() + "]"));
                    continue;
                }
                if (skipSection) continue;
                if (line.startsWith("aws_access_key_id")) {
                    options.setAWSAccessKeyId(line.substring(line.indexOf("=") + 1).trim());
                } else if (line.startsWith("aws_secret_access_key")) {
                    options.setAWSSecretKey(line.substring(line.indexOf("=") + 1).trim());
                }
            }
        } catch (Exception e) {
            // ignore - let other credential-discovery processes have a crack
        }
    }

}
