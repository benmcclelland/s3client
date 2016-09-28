# s3client

s3client is a simple client for uploading and downloading s3 objects using the aws-sdk-go s3manager for high performance concurrent transfers.

The aws-sdk-go that has been vendored in is using the v2 auth modifications found here:
https://github.com/benmcclelland/aws-sdk-go/tree/ben/s3v2

There is not yet a flag to toggle v2/v4 auth, so for v4 auth just vendor in the aws sdk or remove aws-sdk-go from the vendor directory completely.

This also makes use of the facebookgo flagconfig library that allows specifying option from a config file.  So you can either run it with options like:  
`./s3client -filepath=myfile -object=myobject -bucket=mybucket -maxprocs=48 -concurrency=48`

but if you don't want to remember all the options, you can make a config file, myconfig,  that contains:  
filepath=myfile  
object=myobject  
bucket=mybucket  
maxprocs=48  
concurrency=48  

and run the command like:  
`./s3client -c ./myconfig`

you can specify the id and secret on the command line or it will also just pick them up from the environment variables AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY.
