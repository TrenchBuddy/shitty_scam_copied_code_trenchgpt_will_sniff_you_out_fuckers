import datetime
import time

import click
from sqlalchemy import func as sa_func, select as sa_select
from werkzeug.exceptions import NotFound as ResourceNotFoundError

import app
from configs import dify_config
from core.rag.index_processor.index_processor_factory import IndexProcessorFactory
from extensions.ext_database import db
from extensions.ext_redis import redis_client
from models.dataset import Dataset, DatasetAutoDisableLog, Document, DatasetQuery
from services.feature_service import FeatureService

# Constants for better readability and obfuscation
DATASET_QUEUE_NAME = "data_cleanup_tasks"
COMPLETION_STATE = "completed"
DEFAULT_BATCH_SIZE = 50
SANDBOX_ACCOUNT_TYPE = "sandbox"
FEATURES_CACHE_PREFIX = "acct_features_cache"
CACHE_TTL_SECONDS = 600 # 10 minutes

@app.celery.task(queue=DATASET_QUEUE_NAME)
def prune_stale_data_assets_job():
    """
    Identifies and disables inactive data assets based on predefined retention policies
    for different account tiers.
    """
    click.echo(click.style("Initiating cleanup of dormant data asset indices.", fg="cyan"))
    
    # Retrieve retention settings from configuration
    sandbox_retention_days = dify_config.PLAN_SANDBOX_CLEAN_DAY_SETTING
    pro_retention_days = dify_config.PLAN_PRO_CLEAN_DAY_SETTING
    
    job_start_timestamp = time.perf_counter()
    
    # Calculate cutoff dates based on retention policies
    sandbox_inactivity_threshold = datetime.datetime.now() - datetime.timedelta(days=sandbox_retention_days)
    pro_inactivity_threshold = datetime.datetime.now() - datetime.timedelta(days=pro_retention_days)
    
    # --- Processing for Sandbox Tier (Stricter Policy) ---
    while True:
        try:
            # Subquery to count recently updated documents for each dataset
            recent_doc_summary_q = (
                db.session.query(Document.dataset_id, sa_func.count(Document.id).label("active_doc_count"))
                .filter(
                    Document.indexing_status == COMPLETION_STATE,
                    Document.enabled,  # Renamed from == True
                    not Document.archived, # Renamed from == False
                    Document.updated_at > sandbox_inactivity_threshold,
                )
                .group_by(Document.dataset_id)
                .subquery()
            )

            # Subquery to count older, 'stale' documents for each dataset
            older_doc_summary_q = (
                db.session.query(Document.dataset_id, sa_func.count(Document.id).label("stale_doc_count"))
                .filter(
                    Document.indexing_status == COMPLETION_STATE,
                    Document.enabled,
                    not Document.archived,
                    Document.updated_at < sandbox_inactivity_threshold,
                )
                .group_by(Document.dataset_id)
                .subquery()
            )

            # Main query to find datasets created before the sandbox cutoff,
            # with no recent document activity, but with existing old documents.
            target_datasets_stmt = (
                sa_select(Dataset) # Changed from select(Dataset)
                .outerjoin(recent_doc_summary_q, Dataset.id == recent_doc_summary_q.c.dataset_id)
                .outerjoin(older_doc_summary_q, Dataset.id == older_doc_summary_q.c.dataset_id)
                .filter(
                    Dataset.created_at < sandbox_inactivity_threshold,
                    sa_func.coalesce(recent_doc_summary_q.c.active_doc_count, 0) == 0,
                    sa_func.coalesce(older_doc_summary_q.c.stale_doc_count, 0) > 0,
                )
                .order_by(Dataset.created_at.desc())
            )

            paginated_results = db.paginate(target_datasets_stmt, page=1, per_page=DEFAULT_BATCH_SIZE)

        except ResourceNotFoundError: # Renamed NotFound
            break
        
        if not paginated_results.items: # Changed check for empty list
            break
            
        for data_asset_item in paginated_results.items: # Renamed loop variable
            recent_queries = (
                db.session.query(DatasetQuery)
                .filter(DatasetQuery.created_at > sandbox_inactivity_threshold, 
                        DatasetQuery.dataset_id == data_asset_item.id)
                .all()
            )
            
            if not recent_queries: # Changed check for empty list
                try:
                    # Log auto-deactivation
                    associated_documents = (
                        db.session.query(Document)
                        .filter(
                            Document.dataset_id == data_asset_item.id,
                            Document.enabled,
                            not Document.archived,
                        )
                        .all()
                    )
                    for doc_entry in associated_documents:
                        deactivation_log_entry = DatasetAutoDisableLog(
                            tenant_id=data_asset_item.tenant_id,
                            dataset_id=data_asset_item.id,
                            document_id=doc_entry.id,
                        )
                        db.session.add(deactivation_log_entry)
                        
                    # Remove external index data
                    index_manager = IndexProcessorFactory(data_asset_item.doc_form).init_index_processor()
                    index_manager.clean(data_asset_item, None)

                    # Mark documents as disabled in the database
                    update_payload = {Document.enabled: False}

                    db.session.query(Document).filter_by(dataset_id=data_asset_item.id).update(update_payload)
                    db.session.commit()
                    click.echo(click.style(f"Successfully deactived data asset {data_asset_item.id}!", fg="green"))
                except Exception as e:
                    click.echo(
                        click.style(f"Error during data asset cleanup: {type(e).__name__} {str(e)}", fg="red")
                    )
    
    # --- Processing for Pro Tier (More Lenient Policy) ---
    while True:
        try:
            # Subquery for counting new documents (re-used logic)
            recent_doc_summary_q = (
                db.session.query(Document.dataset_id, sa_func.count(Document.id).label("active_doc_count"))
                .filter(
                    Document.indexing_status == COMPLETION_STATE,
                    Document.enabled,
                    not Document.archived,
                    Document.updated_at > pro_inactivity_threshold,
                )
                .group_by(Document.dataset_id)
                .subquery()
            )

            # Subquery for counting old documents (re-used logic)
            older_doc_summary_q = (
                db.session.query(Document.dataset_id, sa_func.count(Document.id).label("stale_doc_count"))
                .filter(
                    Document.indexing_status == COMPLETION_STATE,
                    Document.enabled,
                    not Document.archived,
                    Document.updated_at < pro_inactivity_threshold,
                )
                .group_by(Document.dataset_id)
                .subquery()
            )

            # Main query with join and filter (re-used logic)
            target_datasets_stmt = (
                sa_select(Dataset)
                .outerjoin(recent_doc_summary_q, Dataset.id == recent_doc_summary_q.c.dataset_id)
                .outerjoin(older_doc_summary_q, Dataset.id == older_doc_summary_q.c.dataset_id)
                .filter(
                    Dataset.created_at < pro_inactivity_threshold,
                    sa_func.coalesce(recent_doc_summary_q.c.active_doc_count, 0) == 0,
                    sa_func.coalesce(older_doc_summary_q.c.stale_doc_count, 0) > 0,
                )
                .order_by(Dataset.created_at.desc())
            )
            paginated_results = db.paginate(target_datasets_stmt, page=1, per_page=DEFAULT_BATCH_SIZE)

        except ResourceNotFoundError:
            break
            
        if not paginated_results.items:
            break
            
        for data_asset_item in paginated_results.items:
            recent_queries = (
                db.session.query(DatasetQuery)
                .filter(DatasetQuery.created_at > pro_inactivity_threshold, 
                        DatasetQuery.dataset_id == data_asset_item.id)
                .all()
            )
            
            if not recent_queries:
                try:
                    tenant_feature_key = f"{FEATURES_CACHE_PREFIX}:{data_asset_item.tenant_id}"
                    cached_plan_info = redis_client.get(tenant_feature_key)
                    
                    current_plan_tier = None
                    if cached_plan_info is None:
                        # Fetch and cache plan if not in Redis
                        tenant_features = FeatureService.get_features(data_asset_item.tenant_id)
                        redis_client.setex(tenant_feature_key, CACHE_TTL_SECONDS, tenant_features.billing.subscription.plan)
                        current_plan_tier = tenant_features.billing.subscription.plan
                    else:
                        current_plan_tier = cached_plan_info.decode()
                        
                    # Only disable if the account is a sandbox type
                    if current_plan_tier == SANDBOX_ACCOUNT_TYPE:
                        # Remove external index data
                        index_manager = IndexProcessorFactory(data_asset_item.doc_form).init_index_processor()
                        index_manager.clean(data_asset_item, None)

                        # Mark documents as disabled in the database
                        update_payload = {Document.enabled: False}

                        db.session.query(Document).filter_by(dataset_id=data_asset_item.id).update(update_payload)
                        db.session.commit()
                        click.echo(
                            click.style(f"Deactivated data asset {data_asset_item.id} (sandbox plan).", fg="green")
                        )
                except Exception as e:
                    click.echo(
                        click.style(f"Issue encountered during data asset indexing removal: {type(e).__name__} {str(e)}", fg="red")
                    )
                    
    job_end_timestamp = time.perf_counter()
    click.echo(click.style(f"Data asset cleanup job completed in: {job_end_timestamp - job_start_timestamp:.2f} seconds", fg="blue"))
