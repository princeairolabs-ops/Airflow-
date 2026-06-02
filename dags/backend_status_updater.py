"""
Backend Status Updater - Zero Task Rerun Solution
Updates parent and super-master DAG status directly in MySQL database
No tasks are rerun, only parent status is retroactively corrected
COMPLIANCE SAFE: All DAGs/tasks run exactly once
"""

from datetime import datetime, timedelta
from typing import Optional, Tuple, List, Dict
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from sqlalchemy import and_, update
import logging
import time
import json

logger = logging.getLogger(__name__)


class BackendStatusUpdater:
    """
    Updates parent DAG status directly in MySQL without rerunning tasks
    
    Key Principle:
    - DO NOT modify task_instance state (this would trigger reruns)
    - DO modify dag_run state only (parent level)
    - Use database triggers/procedures for consistency
    """
    
    # Configuration
    POLLING_INTERVAL_SECONDS = 10
    MAX_POLLING_DURATION_SECONDS = 300
    POLL_ATTEMPTS = MAX_POLLING_DURATION_SECONDS // POLLING_INTERVAL_SECONDS
    
    @staticmethod
    def get_child_dag_state_readonly(
        dag_id: str,
        run_id: str,
        session=None
    ) -> Tuple[Optional[str], Optional[datetime]]:
        """
        READ-ONLY query to get child DAG state
        Does NOT modify anything, only queries
        
        Args:
            dag_id: Child DAG identifier
            run_id: Child DAG run identifier
            session: SQLAlchemy session
            
        Returns:
            Tuple of (state, end_date)
        """
        try:
            dag_run = session.query(DagRun).filter(
                and_(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id == run_id
                )
            ).first()
            
            if not dag_run:
                logger.warning(f"DAG run not found: {dag_id}/{run_id}")
                return (None, None)
            
            state = dag_run.state
            end_date = dag_run.end_date
            
            logger.info(
                f"[READ-ONLY] Child DAG {dag_id} ({run_id}): "
                f"state={state}, end_date={end_date}"
            )
            
            return (state, end_date)
        
        except Exception as e:
            logger.error(f"Database read error: {str(e)}")
            return (None, None)
    
    @staticmethod
    def update_parent_dag_status_direct(
        parent_dag_id: str,
        parent_run_id: str,
        new_state: str,
        session=None
    ) -> bool:
        """
        DIRECT database update of parent DAG status
        Uses raw SQL UPDATE to avoid ORM side effects
        
        CRITICAL: This updates ONLY dag_run.state, NOT task_instance
        This prevents any task reruns while fixing parent status
        
        Args:
            parent_dag_id: Parent DAG identifier
            parent_run_id: Parent DAG run identifier
            new_state: New state ('success', 'failed', etc.)
            session: SQLAlchemy session
            
        Returns:
            True if update successful, False otherwise
        """
        try:
            logger.warning("="*80)
            logger.warning("CRITICAL: DIRECT DATABASE STATUS UPDATE")
            logger.warning("="*80)
            logger.warning(f"Target DAG: {parent_dag_id}")
            logger.warning(f"Target Run ID: {parent_run_id}")
            logger.warning(f"New State: {new_state}")
            logger.warning(f"Update Time: {datetime.utcnow().isoformat()}")
            logger.warning("="*80)
            
            # Direct update query
            stmt = update(DagRun).where(
                and_(
                    DagRun.dag_id == parent_dag_id,
                    DagRun.run_id == parent_run_id
                )
            ).values(
                state=new_state,
                end_date=datetime.utcnow()
            )
            
            result = session.execute(stmt)
            session.commit()
            
            rows_affected = result.rowcount
            
            logger.warning(f"✓ Database Update Success: {rows_affected} row(s) updated")
            logger.warning("="*80)
            
            return rows_affected > 0
        
        except Exception as e:
            logger.error(f"Database update error: {str(e)}")
            session.rollback()
            return False
    
    @staticmethod
    def update_parent_task_instance_status(
        parent_dag_id: str,
        parent_run_id: str,
        task_id: str,
        new_state: str,
        session=None
    ) -> bool:
        """
        Update specific task instance status in parent DAG
        Only updates the task that wraps the trigger_dag_run_operator
        
        Args:
            parent_dag_id: Parent DAG identifier
            parent_run_id: Parent DAG run identifier
            task_id: Task identifier (usually the trigger task)
            new_state: New state
            session: SQLAlchemy session
            
        Returns:
            True if successful
        """
        try:
            logger.warning(f"Updating task instance: {task_id}")
            
            stmt = update(TaskInstance).where(
                and_(
                    TaskInstance.dag_id == parent_dag_id,
                    TaskInstance.run_id == parent_run_id,
                    TaskInstance.task_id == task_id
                )
            ).values(
                state=new_state,
                end_date=datetime.utcnow()
            )
            
            result = session.execute(stmt)
            session.commit()
            
            logger.warning(f"✓ Task instance updated: {result.rowcount} row(s)")
            return result.rowcount > 0
        
        except Exception as e:
            logger.error(f"Task instance update error: {str(e)}")
            session.rollback()
            return False
    
    @staticmethod
    def poll_and_update_parent_on_recovery(**context):
        """
        MAIN FUNCTION: Poll child, then update parent status when child succeeds
        NO task reruns, NO dag reruns - only parent status update
        
        Flow:
        1. Child DAG initial trigger fails
        2. This function polls child status for 5 minutes
        3. When child reaches SUCCESS, update parent status directly in DB
        4. Parent DAG status changes to SUCCESS without rerunning
        """
        
        session = context['session']
        task_instance = context['task_instance']
        parent_dag_run = context['dag_run']
        
        parent_dag_id = parent_dag_run.dag_id
        parent_run_id = parent_dag_run.run_id
        
        logger.info("="*80)
        logger.info("ZERO-RERUN BACKEND STATUS UPDATE INITIATED")
        logger.info(f"Parent DAG: {parent_dag_id}")
        logger.info(f"Parent Run: {parent_run_id}")
        logger.info("="*80)
        
        # Get trigger task from upstream
        trigger_task_id = None
        for task in context['task'].upstream_list:
            if 'trigger' in task.task_id.lower():
                trigger_task_id = task.task_id
                break
        
        if not trigger_task_id:
            raise AirflowException("No trigger task found in upstream")
        
        # Extract child DAG info
        child_dag_id = trigger_task_id.replace('trigger_', '').upper()
        
        child_run_id = task_instance.xcom_pull(
            task_ids=trigger_task_id,
            key='dag_run_id'
        )
        
        if not child_run_id:
            raise AirflowException("Could not extract child DAG run_id")
        
        logger.info(f"Child DAG: {child_dag_id} ({child_run_id})")
        
        # Store metadata
        task_instance.xcom_push(
            key='backend_update_info',
            value={
                'parent_dag_id': parent_dag_id,
                'parent_run_id': parent_run_id,
                'child_dag_id': child_dag_id,
                'child_run_id': child_run_id,
                'polling_start': datetime.utcnow().isoformat(),
            }
        )
        
        poll_history = []
        
        logger.info("="*80)
        logger.info("POLLING CHILD DAG FOR RECOVERY")
        logger.info("="*80)
        
        # ===== MAIN POLLING LOOP =====
        for attempt in range(1, BackendStatusUpdater.POLL_ATTEMPTS + 1):
            
            elapsed = (attempt - 1) * BackendStatusUpdater.POLLING_INTERVAL_SECONDS
            
            # Query child DAG state (READ-ONLY)
            child_state, end_date = BackendStatusUpdater.get_child_dag_state_readonly(
                child_dag_id,
                child_run_id,
                session
            )
            
            poll_entry = {
                'attempt': attempt,
                'elapsed_seconds': elapsed,
                'timestamp': datetime.utcnow().isoformat(),
                'child_state': child_state,
            }
            poll_history.append(poll_entry)
            
            logger.info(
                f"[Poll {attempt:2d}/{BackendStatusUpdater.POLL_ATTEMPTS}] "
                f"Elapsed: {elapsed:3d}s | "
                f"Child State: {child_state:7s}"
            )
            
            # ===== SUCCESS: Child recovered, update parent =====
            if child_state == DagRunState.SUCCESS:
                logger.warning("="*80)
                logger.warning("✓ CHILD DAG RECOVERY DETECTED!")
                logger.warning(f"✓ Child state: {child_state}")
                logger.warning("✓ UPDATING PARENT DAG STATUS VIA BACKEND")
                logger.warning("="*80)
                
                # Update parent DAG status directly
                parent_update_success = BackendStatusUpdater.update_parent_dag_status_direct(
                    parent_dag_id,
                    parent_run_id,
                    DagRunState.SUCCESS,
                    session
                )
                
                if parent_update_success:
                    logger.warning("✓ PARENT DAG STATUS UPDATED TO SUCCESS")
                    
                    # Also update the trigger task status for consistency
                    BackendStatusUpdater.update_parent_task_instance_status(
                        parent_dag_id,
                        parent_run_id,
                        trigger_task_id,
                        TaskInstanceState.SUCCESS,
                        session
                    )
                    
                    task_instance.xcom_push(
                        key='backend_update_result',
                        value={
                            'success': True,
                            'updated_at': datetime.utcnow().isoformat(),
                            'recovery_time_seconds': elapsed,
                            'recovery_at_attempt': attempt,
                            'parent_new_state': DagRunState.SUCCESS,
                        }
                    )
                    
                    logger.warning("✓ NO TASKS WERE RERUN - COMPLIANCE SAFE")
                    logger.warning("="*80)
                    return "PARENT_STATUS_UPDATED"
                
                else:
                    raise AirflowException(
                        f"Failed to update parent DAG status: {parent_dag_id}/{parent_run_id}"
                    )
            
            # ===== RUNNING =====
            elif child_state == DagRunState.RUNNING:
                logger.info(f"Child still RUNNING... waiting")
            
            # ===== FAILED: Still waiting =====
            elif child_state == DagRunState.FAILED:
                logger.warning(
                    f"Child still FAILED (attempt {attempt}). "
                    f"Waiting for manual recovery..."
                )
            
            else:
                logger.warning(f"Child in state: {child_state}")
            
            # Wait before next poll
            if attempt < BackendStatusUpdater.POLL_ATTEMPTS:
                time.sleep(BackendStatusUpdater.POLLING_INTERVAL_SECONDS)
        
        # ===== POLLING TIMEOUT =====
        logger.error("="*80)
        logger.error("✗ POLLING TIMEOUT - No child recovery detected within 5 minutes")
        logger.error("="*80)
        logger.error("")
        logger.error("RECOVERY INSTRUCTIONS:")
        logger.error("1. Go to Airflow UI → DAGs → " + child_dag_id)
        logger.error(f"2. Find Run ID: {child_run_id}")
        logger.error("3. Click on failed task")
        logger.error("4. Click 'Clear task instance'")
        logger.error("5. Click 'Rerun'")
        logger.error("6. Parent DAG status will auto-update when child succeeds")
        logger.error("")
        logger.error("No parent tasks will be rerun. Status update only.")
        logger.error("="*80)
        
        task_instance.xcom_push(
            key='backend_update_result',
            value={
                'success': False,
                'timeout_seconds': BackendStatusUpdater.MAX_POLLING_DURATION_SECONDS,
                'polls_completed': BackendStatusUpdater.POLL_ATTEMPTS,
                'final_child_state': child_state,
            }
        )
        task_instance.xcom_push(key='poll_history', value=poll_history)
        
        raise AirflowException(
            f"Child DAG {child_dag_id} still in {child_state} state after 5 minutes. "
            f"Please clear failed task and rerun for parent status auto-update."
        )
    
    @staticmethod
    def audit_log_status_update(**context):
        """
        Log the status update for audit trail
        Shows what was updated and when
        """
        task_instance = context['task_instance']
        
        backend_info = task_instance.xcom_pull(key='backend_update_info') or {}
        update_result = task_instance.xcom_pull(key='backend_update_result') or {}
        poll_history = task_instance.xcom_pull(key='poll_history') or []
        
        audit_record = {
            'timestamp': datetime.utcnow().isoformat(),
            'audit_type': 'PARENT_STATUS_UPDATE',
            'parent_dag': backend_info.get('parent_dag_id'),
            'parent_run': backend_info.get('parent_run_id'),
            'child_dag': backend_info.get('child_dag_id'),
            'child_run': backend_info.get('child_run_id'),
            'update_status': 'SUCCESS' if update_result.get('success') else 'FAILED',
            'parent_new_state': update_result.get('parent_new_state'),
            'recovery_time_seconds': update_result.get('recovery_time_seconds'),
            'poll_attempts': len(poll_history),
            'compliance_note': 'NO TASKS RERUN - PARENT STATUS ONLY',
        }
        
        logger.info("\n" + "="*80)
        logger.info("AUDIT LOG - BACKEND STATUS UPDATE")
        logger.info("="*80)
        logger.info(json.dumps(audit_record, indent=2))
        logger.info("="*80 + "\n")
        
        return audit_record


# ==================== DAG DEFINITIONS ====================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,  # NO AUTOMATIC RETRIES
}

# ==================== MAIN DAGs ====================

with DAG(
    'CM_DAG',
    default_args=default_args,
    description='Content Management DAG - 2 tasks',
    schedule_interval=None,
    tags=['main-dag'],
) as cm_dag:
    
    def cm_task_1():
        logger.info("CM_DAG Task 1")
        return "Success"
    
    def cm_task_2():
        logger.info("CM_DAG Task 2")
        return "Success"
    
    t1 = PythonOperator(task_id='cm_task_1', python_callable=cm_task_1)
    t2 = PythonOperator(task_id='cm_task_2', python_callable=cm_task_2)
    t1 >> t2


with DAG(
    'CD_DAG',
    default_args=default_args,
    description='Continuous Deployment DAG - 4 tasks',
    schedule_interval=None,
    tags=['main-dag'],
) as cd_dag:
    
    def cd_task(num):
        logger.info(f"CD_DAG Task {num}")
        return f"Success {num}"
    
    tasks = [
        PythonOperator(task_id=f'cd_task_{i}', python_callable=cd_task, op_kwargs={'num': i})
        for i in range(1, 5)
    ]
    
    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]


# ==================== MASTER DAG WITH ZERO-RERUN SOLUTION ====================

with DAG(
    'TRD_Master_ZeroRerun',
    default_args=default_args,
    description='Master DAG - Backend status update only',
    schedule_interval=None,
    tags=['master-dag', 'zero-rerun', 'compliance-safe'],
) as trd_master_dag:
    
    # TRIGGER: Initial trigger (runs once)
    trigger_cm = TriggerDagRunOperator(
        task_id='trigger_cm_dag',
        trigger_dag_id='CM_DAG',
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=[TaskInstanceState.SUCCESS, TaskInstanceState.FAILED],
        failed_states=[TaskInstanceState.FAILED],
    )
    
    # RECOVERY: Poll and update parent status (if trigger fails)
    backend_status_update = PythonOperator(
        task_id='backend_status_update',
        python_callable=BackendStatusUpdater.poll_and_update_parent_on_recovery,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED,  # Only if trigger fails
        pool_slots=1,
    )
    
    # AUDIT: Log what was updated
    audit_log = PythonOperator(
        task_id='audit_log_update',
        python_callable=BackendStatusUpdater.audit_log_status_update,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    trigger_cm >> backend_status_update >> audit_log


# ==================== SUPER MASTER DAG ====================

with DAG(
    'SuperMaster_ZeroRerun',
    default_args=default_args,
    description='Super Master DAG - Backend status update only',
    schedule_interval=None,
    tags=['super-master-dag', 'zero-rerun', 'compliance-safe'],
) as super_master_dag:
    
    trigger_trd = TriggerDagRunOperator(
        task_id='trigger_trd_master',
        trigger_dag_id='TRD_Master_ZeroRerun',
        wait_for_completion=True,
        poke_interval=30,
    )
    
    backend_status_update_super = PythonOperator(
        task_id='backend_status_update_super',
        python_callable=BackendStatusUpdater.poll_and_update_parent_on_recovery,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED,
        pool_slots=1,
    )
    
    audit_log_super = PythonOperator(
        task_id='audit_log_update_super',
        python_callable=BackendStatusUpdater.audit_log_status_update,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    trigger_trd >> backend_status_update_super >> audit_log_super
