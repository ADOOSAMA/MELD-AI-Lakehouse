"""
Azure Databricks Lakehouse Deployment Script

Automated deployment of Azure Databricks lakehouse ETL system
"""

import yaml
import json
import logging
import subprocess
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
import requests
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi
from databricks_cli.clusters.api import ClustersApi

class AzureDatabricksDeployer:
    """Azure Databricks deployer"""
    
    def __init__(self, config_path: str = "config/databricks_config.yaml"):
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
        self.api_client = None
        
    def _load_config(self, config_path: str) -> dict:
        """Load configuration file"""
        try:
            with open(config_path, 'r', encoding='utf-8') as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise Exception(f"Failed to load config: {e}")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logger"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger
    
    def initialize_api_client(self) -> bool:
        """Initialize Databricks API client"""
        try:
            databricks_config = self.config['databricks']
            
            self.api_client = ApiClient(
                host=databricks_config['workspace_url'],
                token=databricks_config['access_token']
            )
            
            self.logger.info("Databricks API client initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize API client: {e}")
            return False
    
    def create_cluster(self) -> str:
        """Create Databricks cluster"""
        try:
            if not self.api_client:
                if not self.initialize_api_client():
                    return None
            
            clusters_api = ClustersApi(self.api_client)
            cluster_config = self.config['databricks']['cluster']
            
            cluster_spec = {
                "cluster_name": cluster_config['name'],
                "spark_version": cluster_config['spark_version'],
                "node_type_id": cluster_config['node_type_id'],
                "num_workers": cluster_config['num_workers'],
                "autotermination_minutes": cluster_config['autotermination_minutes'],
                "spark_conf": {
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.databricks.delta.retentionDurationCheck.enabled": "false"
                },
                "libraries": [
                    {
                        "pypi": {
                            "package": "pandas"
                        }
                    },
                    {
                        "pypi": {
                            "package": "pyyaml"
                        }
                    },
                    {
                        "pypi": {
                            "package": "requests"
                        }
                    }
                ]
            }
            
            cluster_info = clusters_api.create_cluster(cluster_spec)
            cluster_id = cluster_info['cluster_id']
            
            self.logger.info(f"Cluster created: {cluster_id}")
            
            # Wait for cluster startup
            self._wait_for_cluster_ready(cluster_id)
            
            return cluster_id
            
        except Exception as e:
            self.logger.error(f"Failed to create cluster: {e}")
            return None
    
    def _wait_for_cluster_ready(self, cluster_id: str, timeout_minutes: 10):
        """Wait for cluster to be ready"""
        try:
            clusters_api = ClustersApi(self.api_client)
            
            start_time = time.time()
            timeout_seconds = timeout_minutes * 60
            
            while time.time() - start_time < timeout_seconds:
                cluster_info = clusters_api.get_cluster(cluster_id)
                state = cluster_info['state']
                
                if state == 'RUNNING':
                    self.logger.info(f"Cluster {cluster_id} is ready")
                    return True
                elif state in ['TERMINATED', 'ERROR']:
                    self.logger.error(f"Cluster {cluster_id} failed to start: {state}")
                    return False
                
                time.sleep(30)  # Wait 30 seconds before retry
            
            self.logger.error(f"Cluster {cluster_id} startup timeout")
            return False
            
        except Exception as e:
            self.logger.error(f"Error waiting for cluster: {e}")
            return False
    
    def create_jobs(self, cluster_id: str) -> List[str]:
        """Create Databricks jobs"""
        try:
            if not self.api_client:
                if not self.initialize_api_client():
                    return []
            
            jobs_api = JobsApi(self.api_client)
            job_ids = []
            
            # Bronze ETL job
            bronze_job = self._create_bronze_etl_job(cluster_id)
            bronze_job_id = jobs_api.create_job(bronze_job)['job_id']
            job_ids.append(bronze_job_id)
            
            # Silver ETL job
            silver_job = self._create_silver_etl_job(cluster_id)
            silver_job_id = jobs_api.create_job(silver_job)['job_id']
            job_ids.append(silver_job_id)
            
            # Gold ETL job
            gold_job = self._create_gold_etl_job(cluster_id)
            gold_job_id = jobs_api.create_job(gold_job)['job_id']
            job_ids.append(gold_job_id)
            
            self.logger.info(f"Created {len(job_ids)} jobs")
            return job_ids
            
        except Exception as e:
            self.logger.error(f"Failed to create jobs: {e}")
            return []
    
    def _create_bronze_etl_job(self, cluster_id: str) -> dict:
        """Create Bronze ETL job"""
        return {
            "name": "ICU Bronze ETL",
            "new_cluster": {
                "cluster_id": cluster_id
            },
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/your-repo/azure_databricks_lakehouse/notebooks/01_bronze_etl",
                "base_parameters": {
                    "bronze_catalog": self.config['databricks']['storage']['delta_tables']['bronze_catalog']
                }
            },
            "schedule": {
                "quartz_cron_expression": self.config['databricks']['jobs']['bronze_etl']['schedule'],
                "timezone_id": "UTC"
            },
            "timeout_seconds": self.config['databricks']['jobs']['bronze_etl']['timeout_seconds'],
            "max_concurrent_runs": 1
        }
    
    def _create_silver_etl_job(self, cluster_id: str) -> dict:
        """Create Silver ETL job"""
        return {
            "name": "ICU Silver ETL",
            "new_cluster": {
                "cluster_id": cluster_id
            },
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/your-repo/azure_databricks_lakehouse/notebooks/02_silver_etl",
                "base_parameters": {
                    "silver_catalog": self.config['databricks']['storage']['delta_tables']['silver_catalog']
                }
            },
            "schedule": {
                "quartz_cron_expression": self.config['databricks']['jobs']['silver_etl']['schedule'],
                "timezone_id": "UTC"
            },
            "timeout_seconds": self.config['databricks']['jobs']['silver_etl']['timeout_seconds'],
            "max_concurrent_runs": 1,
            "depends_on": [
                {
                    "job_id": "bronze_etl_job_id"  # Need to get bronze job ID first
                }
            ]
        }
    
    def _create_gold_etl_job(self, cluster_id: str) -> dict:
        """Create Gold ETL job"""
        return {
            "name": "ICU Gold ETL",
            "new_cluster": {
                "cluster_id": cluster_id
            },
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/your-repo/azure_databricks_lakehouse/notebooks/03_gold_etl",
                "base_parameters": {
                    "gold_catalog": self.config['databricks']['storage']['delta_tables']['gold_catalog']
                }
            },
            "schedule": {
                "quartz_cron_expression": self.config['databricks']['jobs']['gold_etl']['schedule'],
                "timezone_id": "UTC"
            },
            "timeout_seconds": self.config['databricks']['jobs']['gold_etl']['timeout_seconds'],
            "max_concurrent_runs": 1,
            "depends_on": [
                {
                    "job_id": "silver_etl_job_id"  # Need to get silver job ID first
                }
            ]
        }
    
    def setup_sql_warehouse(self) -> str:
        """Setup SQL warehouse"""
        try:
            # Create SQL warehouse configuration
            warehouse_config = {
                "name": "ICU-Analytics-Warehouse",
                "cluster_size": "Small",
                "min_num_clusters": 1,
                "max_num_clusters": 3,
                "auto_stop_mins": 30,
                "enable_photon": True,
                "enable_serverless_compute": True
            }
            
            # Use REST API to create SQL warehouse
            headers = {
                'Authorization': f'Bearer {self.config["databricks"]["access_token"]}',
                'Content-Type': 'application/json'
            }
            
            url = f"{self.config['databricks']['workspace_url']}/api/2.0/sql/warehouses"
            
            response = requests.post(url, headers=headers, json=warehouse_config)
            response.raise_for_status()
            
            warehouse_info = response.json()
            warehouse_id = warehouse_info['id']
            
            self.logger.info(f"SQL warehouse created: {warehouse_id}")
            return warehouse_id
            
        except Exception as e:
            self.logger.error(f"Failed to create SQL warehouse: {e}")
            return None
    
    def deploy_notebooks(self) -> bool:
        """Deploy notebooks to workspace"""
        try:
            # Use Databricks CLI to deploy notebooks
            notebooks_path = "notebooks/"
            workspace_path = "/Workspace/Repos/your-repo/azure_databricks_lakehouse/"
            
            cmd = [
                "databricks",
                "workspace",
                "import_dir",
                notebooks_path,
                workspace_path,
                "--overwrite"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                self.logger.info("Notebooks deployed successfully")
                return True
            else:
                self.logger.error(f"Failed to deploy notebooks: {result.stderr}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error deploying notebooks: {e}")
            return False
    
    def setup_powerbi_connection(self, warehouse_id: str) -> bool:
        """Setup Power BI connection"""
        try:
            # Get SQL warehouse connection information
            headers = {
                'Authorization': f'Bearer {self.config["databricks"]["access_token"]}',
                'Content-Type': 'application/json'
            }
            
            url = f"{self.config['databricks']['workspace_url']}/api/2.0/sql/warehouses/{warehouse_id}"
            
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            warehouse_info = response.json()
            
            # Generate connection string
            connection_string = f"""
            Server: {warehouse_info['odbc_params']['hostname']}
            Port: {warehouse_info['odbc_params']['port']}
            Database: {warehouse_info['odbc_params']['path']}
            HTTP Path: {warehouse_info['odbc_params']['path']}
            """
            
            self.logger.info("Power BI connection string generated")
            self.logger.info(connection_string)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup Power BI connection: {e}")
            return False
    
    def run_deployment(self) -> bool:
        """Run complete deployment"""
        try:
            self.logger.info("Starting Azure Databricks deployment")
            
            # 1. Initialize API client
            if not self.initialize_api_client():
                return False
            
            # 2. Create cluster
            cluster_id = self.create_cluster()
            if not cluster_id:
                return False
            
            # 3. Deploy notebooks
            if not self.deploy_notebooks():
                return False
            
            # 4. Create jobs
            job_ids = self.create_jobs(cluster_id)
            if not job_ids:
                return False
            
            # 5. Setup SQL warehouse
            warehouse_id = self.setup_sql_warehouse()
            if not warehouse_id:
                return False
            
            # 6. Setup Power BI connection
            if not self.setup_powerbi_connection(warehouse_id):
                return False
            
            self.logger.info("Deployment completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Deployment failed: {e}")
            return False

def main():
    """Main function"""
    deployer = AzureDatabricksDeployer()
    success = deployer.run_deployment()
    
    if success:
        print("✅ Deployment completed successfully!")
        print("📊 Your Azure Databricks lakehouse is ready")
        print("🔗 Connect Power BI using the generated connection string")
    else:
        print("❌ Deployment failed")
        print("📝 Check the logs for details")

if __name__ == "__main__":
    main()

