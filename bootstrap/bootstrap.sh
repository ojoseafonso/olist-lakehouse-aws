# variables
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_NAME="olist-tfstate-${ACCOUNT_ID}"
TABLE_NAME="terraform-state-lock"
REGION="us-east-1"

echo "Account ID: ${ACCOUNT_ID}"
echo "Bucket:     ${BUCKET_NAME}"
echo "Table:      ${TABLE_NAME}"
echo "Region:     ${REGION}"
echo "---"

BUCKET_NAME="olist-tfstate-<account_id>"
if ! aws s3api head-bucket --bucket "$BUCKET_NAME" > /dev/null 2>&1; then
    echo "Creating bucket..."
    aws s3api create-bucket --bucket "$BUCKET_NAME" --region <aws-region> 
else
    echo "Table $BUCKET_NAME already exists."
fi

TABLE_NAME="terraform-state-lock-table"
if ! aws dynamodb describe-table --table-name "$TABLE_NAME" > /dev/null 2>&1; then
    echo "Creating table..."
    aws dynamodb create-table \
        --table-name "$TABLE_NAME" \
    	--attribute-definitions AttributeName=LockID,AttributeType=S \
    	--key-schema AttributeName=LockID,KeyType=HASH \
    	--billing-mode PAY_PER_REQUEST \
    	--region us-east-1
else
    echo "Table $TABLE_NAME already exists."
fi
