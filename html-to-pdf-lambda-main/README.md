# html-to-pdf-lambda

AWS Lambda to generate PDF from HTML

# Getting started

## Setup environment

```
pip3 install -r src/requirements.txt
```

## Deploy

```

sam build

```

```
aws s3api create-bucket --bucket firestorage-pdf --region ap-northeast-1 --create-bucket-configuration LocationConstraint=ap-northeast-1
```

```
aws ecr create-repository --repository-name html-to-pdf-lambda --region ap-northeast-1
```

## 2回目以降
sam build

```
sam deploy \
--stack-name html-to-pdf-lambda-stack \
--region ap-northeast-1   \
--s3-bucket firestorage-pdf \
--capabilities CAPABILITY_NAMED_IAM \
--image-repository 695355811576.dkr.ecr.ap-northeast-1.amazonaws.com/html-to-pdf-lambda
```
