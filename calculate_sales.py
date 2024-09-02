from elasticsearch import Elasticsearch
import json
import hvac

# Vault configuration
vault_url = "https://vault.example.com"
vault_token = "your-vault-token"  # It's recommended to use an environment variable or other secure method to store the token

# Initialize Vault client
client = hvac.Client(url=vault_url, token=vault_token)

def get_credentials_from_vault(secret_path):
    try:
        # Retrieve the secret from Vault
        secret = client.read(secret_path)
        if secret:
            return secret['data']['data']
        else:
            raise Exception("Failed to retrieve secret from Vault.")
    except Exception as e:
        print(f"Error retrieving credentials from Vault: {e}")
        return None

# Define Elasticsearch clusters and their corresponding Vault paths
es_clusters = [
    {
        "name": "Paris",
        "vault_secret_path": "secret/data/elasticsearch/paris",
        "client": None  # Will be initialized after retrieving credentials
    },
    {
        "name": "Hong Kong",
        "vault_secret_path": "secret/data/elasticsearch/hk",
        "client": None  # Will be initialized after retrieving credentials
    },
    {
        "name": "USA",
        "vault_secret_path": "secret/data/elasticsearch/usa",
        "client": None  # Will be initialized after retrieving credentials
    },
    {
        "name": "North",
        "vault_secret_path": "secret/data/elasticsearch/north",
        "client": None  # Will be initialized after retrieving credentials
    },
]

# Define customers with their specific indices and operations
customers = {
    "customer1": {
        "index": "customer1_index",
        "operation_name": "computer_selling"
    },
    "customer2": {
        "index": "customer2_index",
        "operation_name": "constructing_house"
    },
    "customer3": {
        "index": "customer3_index",
        "operation_name": "car_manufacturing"
    },
    # Add other customers with their specific indices and operations here
}

success_field = "payment_received"
success_value = "yes"

def get_sales_data(es_client, index, operation_name, success_field, success_value):
    try:
        # Query for the total number of sales operations
        total_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"operation_name": operation_name}}
                    ]
                }
            }
        }
        total_sales = es_client.count(index=index, body=total_query)['count']

        # Query for the number of successful sales operations
        success_query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"operation_name": operation_name}},
                        {"term": {success_field: success_value}}
                    ]
                }
            }
        }
        successful_sales = es_client.count(index=index, body=success_query)['count']

        return total_sales, successful_sales

    except Exception as e:
        print(f"Error getting sales data for index {index}: {e}")
        return 0, 0

def main():
    final_results = []

    # Initialize Elasticsearch clients for each cluster
    for cluster in es_clusters:
        credentials = get_credentials_from_vault(cluster["vault_secret_path"])
        if credentials:
            cluster["client"] = Elasticsearch(
                [f"https://{cluster['name'].lower()}-es.example.com"],  # Assuming the URL follows this pattern
                http_auth=(credentials["username"], credentials["password"]),
                timeout=30
            )
        else:
            raise Exception(f"Could not retrieve credentials for {cluster['name']}. Exiting.")

    for customer, info in customers.items():
        customer_results = []
        total_sales_all_regions = 0
        successful_sales_all_regions = 0

        for cluster in es_clusters:
            es_client = cluster["client"]
            total_sales, successful_sales = get_sales_data(es_client, info["index"], info["operation_name"], success_field, success_value)

            if total_sales > 0:
                success_percentage = (successful_sales / total_sales) * 100
            else:
                success_percentage = 0.0

            total_sales_all_regions += total_sales
            successful_sales_all_regions += successful_sales

            customer_results.append({
                "region": cluster['name'],
                "total_sales": total_sales,
                "successful_sales": successful_sales,
                "success_percentage": success_percentage
            })

        # Calculate the overall success percentage across all regions for the customer
        if total_sales_all_regions > 0:
            overall_success_percentage = (successful_sales_all_regions / total_sales_all_regions) * 100
        else:
            overall_success_percentage = 0.0

        # Add the overall success percentage to the customer results
        customer_summary = {
            "customer": customer,
            "index": info["index"],
            "operation_name": info["operation_name"],
            "regions": customer_results,
            "total_sales_all_regions": total_sales_all_regions,
            "successful_sales_all_regions": successful_sales_all_regions,
            "overall_success_percentage": overall_success_percentage
        }

        final_results.append(customer_summary)

    # Convert results to JSON
    response = json.dumps(final_results, indent=2)

    # Output the JSON response
    print(response)

if __name__ == "__main__":
    main()
