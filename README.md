# Run Farm Sales Models
dbt run --profiles-dir profile --target prod --models +farm_sales

# Docker build
docker build -t gf-dbt:latest . 

# Run docker at local, for example running dev env for farm sales models
docker run \
  -e REDSHIFT_HOST={REDSHIFT_HOST} \
  -e REDSHIFT_PORT={REDSHIFT_PORT} \
  -e REDSHIFT_DATABASE={REDSHIFT_DATABASE} \
  -e REDSHIFT_SCHEMA={REDSHIFT_SCHEMA} \
  -e REDSHIFT_USER={REDSHIFT_USER} \
  -e REDSHIFT_PASSWORD={REDSHIFT_PASSWORD} \
  -e REDSHIFT_TIMEOUT={REDSHIFT_TIMEOUT} \
  gf-dbt:latest dbt run --profiles-dir profile \
  --target dev --models +farm_sales "# sales-group" 
