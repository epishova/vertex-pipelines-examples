import serving
import run_pipelines

SERVE_CONFIG_FILE = "config/config.yaml.example"
TEMPLATE_FILE = "templates/your_package_name_serve.json"

serving.compile_pipeline(template_path=TEMPLATE_FILE)

run_pipelines.run_pipeline(template_path=TEMPLATE_FILE, config_path=SERVE_CONFIG_FILE)