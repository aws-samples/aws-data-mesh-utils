# Data Mesh Utils Command Line Utility

The Data Mesh Utils library ships with a command line utilty `data-mesh-cli`, which can be used form bash or another terminal shell. This utility provides a simplified interface for working with the utility that operates on commands which route to the correct accounts based upon their context.

## Usage

The command line can be invoked with:

```
> ./data-mesh-cli


aws-data-mesh <command> <args>
   <command> - The command to perform, such as 'create-data-product', or 'request-access'. The command automatically infers the credential context to use.
   <args> - Arguments for the command, using '--<parameter> <value>' syntax`
```

You can view full command usage with:

```
> ./data-mesh-cli help

aws-data-mesh <command> <args>
   <command> - The command to perform, such as 'create-data-product', or 'request-access'. The command automatically infers the credential context to use.
   <args> - Arguments for the command, using '--<parameter> <value>' syntax

   Valid Commands:
      create-data-product
      approve-subscription
      deny-subscription
      modify-subscription
      request-access
      import-subscription
      list-subscriptions
      install-mesh-objects
      enable-account
```

You can view per-command help by calling the utility with the command name, and 'help':

```
> ./data-mesh-cli create-data-product help

data-mesh-cli create-data-product <args>
   Required Arguments:
      * data_mesh_account_id
      * source_database_name
   Optional Arguments:
      * use_credentials
      * log_level - default 'INFO'
      * region_name - default 'us-east-1'
      * credentials_file
      * use_original_table_name
      * expose_table_references_with_suffix
      * expose_data_mesh_db_name
      * sync_mesh_crawler_role_arn
      * sync_mesh_catalog_schedule
      * data_product_name
      * domain
      * table_name_regex
      * create_public_metadata
```

## Credentials Management

Each command takes credentials information as an argument in one of two forms:

### Inline Credentials

Inline credentials consist of AWS Access Keys encoded as a JSON string, in the form `--use_credentials <encoded string>`. The JSON structure looks like:

```
{
	"AccessKeyId": "access key",
	"SecretAccessKey": "secret key",
	"SessionToken": "optional session token value for temporary credentials"
}
```
When string escaped for use on the command line, this argument will look like: 

```
--use_credentials "{\"AccessKeyId\":\"my access key\",\"SecretAccessKey\":\"my secret key\"}"
```

### Credentials File

Encoding JSON in string values is a terrible way to spend time, so instead you can use a credentials file. This file takes exactly the same form as documented in the primary [README](README.md), and an example can be found in [examples/example-creds.json](examples/example-creds.json).

To use a credentials file, you specify the path with argument value `--credentials-file <path-to-file>`.

### Environment Credentials

If neither of these values are provided, then the utility will get credentials from the operating environment using the default credential profile. You will see a message on the command line that this is happening.
