# Dynamodb Client Example

- Run the local dynamodb docker image:
  ```bash
  docker-compose up
  ```
- Install aws CLI [install guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
- Configure aws CLI:

  ```bash
  aws configure
  ```

  Then enter some dummy access--key-id and secret-access-key.

- Create the dynamo table:
  ```bash
  aws dynamodb create-table --endpoint-url http://localhost:8000 --region us-west-1 \
      --table-name User \
      --attribute-definitions AttributeName=id,AttributeType=S \
      --key-schema AttributeName=id,KeyType=HASH \
      --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
  ```
