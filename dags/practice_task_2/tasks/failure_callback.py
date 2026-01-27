def on_failure_callback(context):
    print(f"Task {context['task_instance_key_str']} failed. Alerting...")
