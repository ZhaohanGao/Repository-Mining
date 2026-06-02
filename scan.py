import requests
import time
import os
import csv
import base64
import json
from datetime import datetime
from databricks.sdk import WorkspaceClient

# ==========================================
# 1. Config
# ==========================================

TOKEN_FILE = "token.txt"

try:
    with open(TOKEN_FILE, "r", encoding="utf-8") as file:
        GITHUB_TOKEN = file.read().strip()

    if not GITHUB_TOKEN:
        raise ValueError("GitHub token is empty. Please put your token in token.txt.")

except FileNotFoundError:
    raise FileNotFoundError(
        f"Cannot find {TOKEN_FILE}. Please place token.txt in the same folder as scan.py."
    )

# Databricks authentication - auto-detected from the runtime environment
_ws = WorkspaceClient()
DATABRICKS_HOST = _ws.config.host.rstrip("/")


def _get_databricks_auth_headers():
    """Get fresh Databricks auth headers using the SDK's credential provider.
    This works with all auth types (PAT, notebook-native, OAuth, etc.)."""
    headers = _ws.config.authenticate()  # Returns dict with Authorization header
    headers["Content-Type"] = "application/json"
    return headers


HEADERS = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json",
}

# Databricks Model Serving config
DATABRICKS_MODEL_ENDPOINT = "databricks-gpt-5-4-mini"
DATABRICKS_SERVING_URL = f"{DATABRICKS_HOST}/serving-endpoints/{DATABRICKS_MODEL_ENDPOINT}/invocations"

BASE_SAVE_DIR = "/Workspace/Users/zhaohan.gao@versuni.com/Repository Mining/download"
METADATA_CSV = os.path.join(BASE_SAVE_DIR, "diagram_metadata.csv")
SUMMARY_CSV = os.path.join(BASE_SAVE_DIR, "repo_summary.csv")

# Main limits
MAX_REPOS_PER_QUERY = 1000
MAX_CODE_RESULTS_PER_QUERY = 500
MAX_CANDIDATE_DIAGRAMS = 1000
MAX_DIAGRAMS_PER_REPO = 15

# Lower threshold to collect more candidates; AI will filter later.
DIAGRAM_SCORE_THRESHOLD = 3

MIN_FILE_SIZE = 5 * 1024          # 5 KB (lowered to catch more diagrams)
MAX_FILE_SIZE = 15 * 1024 * 1024  # 15 MB

TARGET_EXTENSIONS = [".png", ".jpg", ".jpeg", ".svg", ".webp", ".drawio"]

# AI classification confidence threshold (0-100).
# Images scoring >= this are kept as architecture diagrams.
AI_CONFIDENCE_THRESHOLD = 60

# ==========================================
# 2. Seed repositories
# ==========================================

SEED_REPOS = [
    # Core MLOps platforms
    "mlflow/mlflow",
    "kubeflow/kubeflow",
    "kubeflow/pipelines",
    "kserve/kserve",
    "SeldonIO/seldon-core",
    "SeldonIO/seldon-server",
    "bentoml/BentoML",
    "feast-dev/feast",
    "Netflix/metaflow",
    "zenml-io/zenml",
    "flyteorg/flyte",
    "polyaxon/polyaxon",
    "clearml/clearml",
    "determined-ai/determined",
    "apache/submarine",
    "deployKF/deployKF",
    "mlrun/mlrun",
    "kedro-org/kedro",
    "tensorflow/tfx",
    "ray-project/ray",
    "microsoft/nni",
    "argoproj/argo-workflows",
    "apache/airflow",
    "dagster-io/dagster",
    "iterative/dvc",
    "iterative/cml",
    "alibaba/PAI",
    "alibaba/pipcook",
    "h2oai/h2o-3",

    # ML/AI platform repos
    "microsoft/FLAML",
    "microsoft/recommenders",
    "microsoft/MLOps",
    "microsoft/MLOpsPython",
    "DataTalksClub/mlops-zoomcamp",
    "GoogleCloudPlatform/mlops-with-vertex-ai",
    "Azure/mlops-v2",
    "aws-samples/aws-mlops-reference-architecture",
    "aws-samples/sagemaker-custom-project-templates",
    "aws-samples/amazon-sagemaker-mlops-workshop",
    "aws-samples/mlops-amazon-sagemaker-devops-with-ml",
    "aws-samples/mlops-workload-orchestrator",
    "open-mmlab/mmdeploy",
    "triton-inference-server/server",
    "NVIDIA/TensorRT",
    "microsoft/onnxruntime",
    "tensorflow/serving",
    "pytorch/serve",

    # Deep learning frameworks (architecture docs)
    "tensorflow/tensorflow",
    "pytorch/pytorch",
    "apache/mxnet",
    "keras-team/keras",

    # Distributed ML / training
    "horovod/horovod",
    "microsoft/DeepSpeed",
    "facebookresearch/fairscale",
    "huggingface/transformers",
    "huggingface/accelerate",
    "Lightning-AI/pytorch-lightning",
    "ray-project/ray",

    # Model serving / inference
    "tensorflow/serving",
    "pytorch/serve",
    "triton-inference-server/server",
    "cortexlabs/cortex",
    "VertaAI/modeldb",
    "combust/mleap",

    # Feature store / data management
    "feast-dev/feast",
    "feathr-ai/feathr",
    "linkedin/feathr",
    "hopsworks/hopsworks",

    # Experiment tracking / model registry
    "mlflow/mlflow",
    "wandb/wandb",
    "neptune-ai/neptune-client",
    "aimhubio/aim",
    "labmlai/labml",

    # AutoML
    "autogluon/autogluon",
    "microsoft/FLAML",
    "automl/auto-sklearn",
    "keras-team/autokeras",
    "EpistasisLab/tpot",
    "optuna/optuna",

    # Data versioning / pipeline
    "iterative/dvc",
    "pachyderm/pachyderm",
    "dagster-io/dagster",
    "PrefectHQ/prefect",
    "apache/airflow",
    "spotify/luigi",

    # ML monitoring / observability
    "evidentlyai/evidently",
    "SeldonIO/alibi-detect",
    "whylabs/whylogs",
    "fiddler-labs/fiddler-auditor",
    "NannyML/nannyml",
    "superwise-ai/elemeta",

    # LLM / GenAI infrastructure
    "langchain-ai/langchain",
    "run-llama/llama_index",
    "vllm-project/vllm",
    "ggerganov/llama.cpp",
    "huggingface/text-generation-inference",
    "ray-project/ray",
    "lm-sys/FastChat",
    "NVIDIA/Megatron-LM",
    "EleutherAI/gpt-neox",
    "microsoft/semantic-kernel",
    "guidance-ai/guidance",
    "BerriAI/litellm",

    # Vector databases (ML infrastructure)
    "milvus-io/milvus",
    "qdrant/qdrant",
    "weaviate/weaviate",
    "chroma-core/chroma",
    "pinecone-io/pinecone-client",

    # ML on Kubernetes
    "kubeflow/kubeflow",
    "ray-project/kuberay",
    "volcano-sh/volcano",
    "apache/submarine",
    "polyaxon/polyaxon",

    # Edge / embedded ML
    "apache/tvm",
    "tensorflow/tflite-micro",
    "NVIDIA/TensorRT",
    "onnx/onnx",
    "alibaba/MNN",

    # Data labeling / annotation
    "HumanSignal/label-studio",
    "heartexlabs/label-studio",
    "UniversalDataTool/universal-data-tool",
    "doccano/doccano",

    # ML reference architectures
    "GoogleCloudPlatform/mlops-on-gcp",
    "GoogleCloudPlatform/vertex-ai-samples",
    "aws/amazon-sagemaker-examples",
    "Azure/MachineLearningNotebooks",
    "databricks/mlops-stacks",
    "databricks/mlflow-export-import",
]

# ==========================================
# 3. Repository search queries
# ==========================================

SEARCH_QUERIES = [
    # Broad MLOps / ML platform
    'mlops stars:>100',
    'mlops architecture stars:>100',
    'mlops platform stars:>100',
    'ml platform stars:>100',
    'ai platform stars:>100',
    'machine learning platform stars:>100',
    'machine learning system stars:>100',
    'machine learning architecture stars:>100',
    'data science platform stars:>100',

    # ML pipeline / workflow
    'ml pipeline stars:>100',
    'machine learning pipeline stars:>100',
    'training pipeline stars:>100',
    'inference pipeline stars:>100',
    'data pipeline machine learning stars:>100',
    'workflow machine learning stars:>100',

    # Model serving / deployment
    'model serving stars:>100',
    'model deployment stars:>100',
    'model inference stars:>100',
    'inference service stars:>100',
    'prediction service stars:>100',
    'online inference stars:>100',
    'batch inference stars:>100',

    # MLOps components
    'feature store stars:>100',
    'model registry stars:>100',
    'model monitoring stars:>100',
    'experiment tracking stars:>100',
    'model training platform stars:>100',

    # Distributed / large-scale ML
    'distributed training stars:>100',
    'distributed machine learning stars:>100',
    'large scale machine learning stars:>100',
    'gpu cluster training stars:>100',
    'model parallelism stars:>100',
    'data parallelism stars:>100',

    # AutoML / HPO
    'automl stars:>100',
    'hyperparameter optimization stars:>100',
    'neural architecture search stars:>100',

    # Data management for ML
    'data versioning stars:>100',
    'data labeling stars:>100',
    'data annotation platform stars:>100',
    'feature engineering stars:>100',
    'data quality machine learning stars:>100',

    # ML monitoring / observability
    'model monitoring stars:>100',
    'ml observability stars:>100',
    'data drift detection stars:>100',
    'model drift stars:>100',

    # Edge / embedded ML
    'edge ml stars:>100',
    'edge inference stars:>100',
    'tinyml stars:>100',
    'model compression stars:>100',
    'model optimization deployment stars:>100',

    # ML on Kubernetes
    'machine learning kubernetes stars:>100',
    'ml kubernetes stars:>100',
    'kubeflow stars:>100',
]

# ==========================================
# 4. Code search queries
# ==========================================

CODE_SEARCH_QUERIES = [
    # Architecture files
    'architecture mlops extension:png',
    'architecture mlops extension:jpg',
    'architecture mlops extension:jpeg',
    'architecture mlops extension:svg',
    'architecture mlops extension:drawio',

    'architecture machine learning extension:png',
    'architecture machine learning extension:svg',
    'architecture machine learning extension:drawio',

    # Pipeline / workflow diagrams
    'pipeline mlops extension:png',
    'pipeline mlops extension:svg',
    'workflow mlops extension:png',
    'workflow mlops extension:svg',

    'pipeline machine learning extension:png',
    'pipeline machine learning extension:svg',
    'workflow machine learning extension:png',
    'workflow machine learning extension:svg',

    # Serving / inference / deployment
    'serving architecture extension:png',
    'serving architecture extension:svg',
    'inference architecture extension:png',
    'inference architecture extension:svg',
    'deployment architecture extension:png',
    'deployment architecture extension:svg',

    # MLOps components
    'feature store architecture extension:png',
    'feature store architecture extension:svg',
    'model registry architecture extension:png',
    'model monitoring architecture extension:png',


    # Distributed training
    'distributed training architecture extension:png',
    'distributed training architecture extension:svg',
    'training architecture extension:png',
    'training architecture extension:svg',

    # Filename-based search
    'filename:architecture mlops',
    'filename:pipeline mlops',
    'filename:workflow mlops',
    'filename:overview mlops',
    'filename:system mlops',
    'filename:deployment mlops',
    'filename:serving mlops',
    'filename:inference mlops',
    'filename:dataflow mlops',
    'filename:data-flow mlops',
    'filename:architecture ml',
    'filename:architecture training',
    'filename:architecture inference',
    'filename:architecture serving',
    'filename:architecture distributed',
    'filename:system-design ml',
    'filename:system_design ml',
]

# ==========================================
# 5. Filtering rules
# ==========================================

REPO_HARD_BLACKLIST = [
    "awesome",
    "roadmap",
    "100-days",
    "interview",
    "cheatsheet",
    "cheat-sheet",
]

POSITIVE_KEYWORDS = {
    "architecture": 5,
    "architectural": 5,
    "arch": 4,
    "system": 3,
    "overview": 3,
    "pipeline": 4,
    "workflow": 3,
    "dataflow": 4,
    "data_flow": 4,
    "data-flow": 4,
    "mlops": 5,
    "deployment": 4,
    "serving": 4,
    "inference": 4,
    "training": 3,
    "component": 3,
    "components": 3,
    "infrastructure": 4,
    "topology": 4,
    "design": 2,
    "diagram": 3,
    "flow": 2,
    "orchestration": 3,
    "reference": 3,
    "model": 2,
    "monitoring": 3,
    "registry": 3,
    "feature": 2,
    "feature-store": 4,
    "feature_store": 4,
    "end-to-end": 3,
    "end_to_end": 3,
    "distributed": 3,
    "cluster": 2,
    "rag": 3,
    "retrieval": 2,
    "embedding": 2,
    "vector": 2,
    "llm": 3,
    "platform": 2,
    "system-design": 4,
    "system_design": 4,
    "high-level": 3,
    "high_level": 3,
}

NEGATIVE_KEYWORDS = {
    "logo": 10,
    "icon": 10,
    "badge": 10,
    "avatar": 10,
    "banner": 8,
    "cover": 6,
    "screenshot": 8,
    "screen": 6,
    "ui": 6,
    "button": 8,
    "dashboard": 5,
    "page": 5,
    "form": 5,
    "login": 8,
    "menu": 6,
    "modal": 6,
    "social": 6,
    "preview": 4,
    "demo": 3,
    "quickstart": 3,
    "tutorial": 2,
    "example": 1,
    "presentation": 4,
    "slide": 4,
    "meme": 10,
    "favicon": 10,
    "sponsor": 10,
    "photo": 8,
    "headshot": 10,
    "profile": 8,
    "emoji": 10,
    "gif": 6,
    "animation": 5,
}

# ==========================================
# 6. Helper functions
# ==========================================

def is_bad_repo(repo):
    repo_name = repo.get("full_name", "").lower()
    return any(word in repo_name for word in REPO_HARD_BLACKLIST)


def is_target_file(file_path):
    return any(file_path.lower().endswith(ext) for ext in TARGET_EXTENSIONS)


def diagram_score(file_path):
    path = file_path.lower()
    score = 0

    for word, weight in POSITIVE_KEYWORDS.items():
        if word in path:
            score += weight

    for word, weight in NEGATIVE_KEYWORDS.items():
        if word in path:
            score -= weight

    if any(folder in path for folder in [
        "docs/",
        "doc/",
        "design/",
        "architecture/",
        "architectures/",
        "images/",
        "image/",
        "img/",
        "assets/",
        "diagrams/",
        "diagram/",
        "static/",
        "_static/",
        "media/",
        "figures/",
        "figure/",
        "resources/",
    ]):
        score += 2

    if "readme" in path:
        score += 1

    return score


def sleep_for_rate_limit(response):
    reset_time = response.headers.get("X-RateLimit-Reset")

    if reset_time:
        try:
            reset_timestamp = int(reset_time)
            wait_seconds = max(reset_timestamp - int(time.time()) + 5, 60)
        except ValueError:
            wait_seconds = 60
    else:
        wait_seconds = 60

    print(f"Rate limited. Sleeping for {wait_seconds} seconds...")
    time.sleep(wait_seconds)


def get_repo_info(repo_full_name):
    url = f"https://api.github.com/repos/{repo_full_name}"

    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
    except Exception as e:
        print(f"Failed to get repo info for {repo_full_name}: {e}")
        return None

    if response.status_code == 200:
        return response.json()

    if response.status_code == 403:
        sleep_for_rate_limit(response)
        return get_repo_info(repo_full_name)

    print(f"Failed to get repo info for {repo_full_name}: {response.status_code}")
    return None


def search_github_repos(query, max_results=300):
    repos = []
    page = 1

    while len(repos) < max_results:
        url = "https://api.github.com/search/repositories"
        params = {
            "q": query,
            "sort": "stars",
            "order": "desc",
            "per_page": 100,
            "page": page,
        }

        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)
        except Exception as e:
            print(f"Repo search failed for query [{query}]: {e}")
            break

        if response.status_code == 403:
            sleep_for_rate_limit(response)
            continue

        if response.status_code != 200:
            print(f"Repo search failed: {response.status_code} - {response.text[:200]}")
            break

        items = response.json().get("items", [])
        if not items:
            break

        repos.extend(items)
        page += 1

        if page > 10:
            break

        time.sleep(2)

    return repos[:max_results]


def search_github_code_for_repos(query, max_results=100):
    """
    Search GitHub code files and extract repositories from matched files.
    GitHub code search API limits to 1000 results max (10 pages of 100).
    """
    repos = []
    seen = set()
    page = 1

    while len(repos) < max_results:
        url = "https://api.github.com/search/code"
        params = {
            "q": query,
            "per_page": 100,
            "page": page,
        }

        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)
        except Exception as e:
            print(f"Code search failed for query [{query}]: {e}")
            break

        if response.status_code == 403:
            sleep_for_rate_limit(response)
            continue

        # GitHub returns 422 when trying to access beyond 1000 results - this is
        # expected and not an error. Just stop paginating for this query.
        if response.status_code == 422:
            print(f"  Reached GitHub 1000-result limit for this query. Moving on.")
            break

        if response.status_code != 200:
            print(f"Code search failed: {response.status_code} - {response.text[:200]}")
            break

        items = response.json().get("items", [])
        if not items:
            break

        for item in items:
            repo_obj = item.get("repository")
            if not repo_obj:
                continue

            full_name = repo_obj.get("full_name")
            if not full_name or full_name in seen:
                continue

            seen.add(full_name)

            full_repo = get_repo_info(full_name)
            if full_repo:
                repos.append(full_repo)

            if len(repos) >= max_results:
                break

            time.sleep(0.5)

        page += 1

        # GitHub code search hard limit: 1000 results = 10 pages of 100
        if page > 10:
            break

        time.sleep(2)

    return repos[:max_results]


def collect_repos():
    """
    Collect repositories from:
    1. Seed repositories.
    2. GitHub repository search.
    3. GitHub code search.
    """
    all_repos = []
    seen = set()

    print("\n==========================================")
    print("Collecting seed repositories...")
    print("==========================================")

    for repo_name in SEED_REPOS:
        if repo_name in seen:
            continue

        repo = get_repo_info(repo_name)

        if repo and repo.get("full_name") not in seen:
            all_repos.append((repo, "seed"))
            seen.add(repo["full_name"])
            print(f"Seed repo added: {repo['full_name']}")

        time.sleep(1)

    print("\n==========================================")
    print("Collecting repositories from GitHub repository search...")
    print("==========================================")

    for query in SEARCH_QUERIES:
        print(f"\nSearching repos with query: {query}")
        repos = search_github_repos(query, max_results=MAX_REPOS_PER_QUERY)

        print(f"  Found {len(repos)} repos for this query.")

        added = 0
        for repo in repos:
            full_name = repo.get("full_name")

            if full_name and full_name not in seen:
                all_repos.append((repo, query))
                seen.add(full_name)
                added += 1

        print(f"  Added {added} new repos.")
        time.sleep(2)

    print("\n==========================================")
    print("Collecting repositories from GitHub code search...")
    print("==========================================")

    for query in CODE_SEARCH_QUERIES:
        print(f"\nSearching code with query: {query}")
        repos = search_github_code_for_repos(
            query,
            max_results=MAX_CODE_RESULTS_PER_QUERY
        )

        print(f"  Found {len(repos)} repos from code search.")

        added = 0
        for repo in repos:
            full_name = repo.get("full_name")

            if full_name and full_name not in seen:
                all_repos.append((repo, f"code_search: {query}"))
                seen.add(full_name)
                added += 1

        print(f"  Added {added} new repos from code search.")
        time.sleep(2)

    print("\n==========================================")
    print(f"Total repositories collected before scanning: {len(all_repos)}")
    print("==========================================")

    return all_repos


def get_repo_tree(repo_full_name, default_branch):
    url = f"https://api.github.com/repos/{repo_full_name}/git/trees/{default_branch}"
    params = {"recursive": "1"}

    try:
        response = requests.get(url, headers=HEADERS, params=params, timeout=60)
    except Exception as e:
        print(f"Tree fetch failed for {repo_full_name}: {e}")
        return []

    if response.status_code == 403:
        sleep_for_rate_limit(response)
        return get_repo_tree(repo_full_name, default_branch)

    if response.status_code != 200:
        print(f"Tree fetch failed for {repo_full_name}: {response.status_code}")
        return []

    data = response.json()

    if data.get("truncated"):
        print(f"Warning: tree truncated for {repo_full_name}. Some files may not be scanned.")

    return data.get("tree", [])


def fetch_file_bytes(raw_url):
    """Fetch file content into memory (bytes). Returns bytes or None on failure."""
    try:
        response = requests.get(
            raw_url,
            headers={"Authorization": f"token {GITHUB_TOKEN}"},
            timeout=60,
        )

        if response.status_code == 403:
            sleep_for_rate_limit(response)
            return fetch_file_bytes(raw_url)

        if response.status_code != 200:
            print(f"  Fetch failed: {response.status_code} {raw_url}")
            return None

        return response.content

    except Exception as e:
        print(f"  Fetch error {raw_url}: {e}")
        return None


def save_file(file_bytes, save_path):
    """Save bytes to disk."""
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    with open(save_path, "wb") as f:
        f.write(file_bytes)


# ==========================================
# 7. AI-based image classification (Databricks Model Serving)
# ==========================================

def classify_image_with_ai(file_bytes, file_path):
    """
    Use Databricks Model Serving (databricks-gpt-5-4-mini) to determine
    if an image is a software/ML architecture diagram.

    Args:
        file_bytes: Raw bytes of the file content (fetched into memory).
        file_path: Original file path (used for extension detection).

    Returns:
        dict with keys:
            - is_architecture_diagram (bool)
            - confidence (int, 0-100)
            - description (str): brief description of the diagram content
            - diagram_type (str): e.g. "system architecture", "pipeline", "deployment", etc.
    """
    ext = os.path.splitext(file_path)[1].lower()

    # Skip non-image files for AI classification (e.g. .drawio, .svg XML)
    if ext in [".drawio"]:
        return {
            "is_architecture_diagram": True,
            "confidence": 70,
            "description": "Drawio file (assumed architecture diagram)",
            "diagram_type": "drawio",
        }

    if ext == ".svg":
        # SVGs can be large XML; check if it looks like a diagram
        try:
            content = file_bytes[:5000].decode("utf-8", errors="ignore")
            diagram_indicators = ["rect", "path", "line", "polygon", "text", "arrow"]
            if any(indicator in content.lower() for indicator in diagram_indicators):
                return {
                    "is_architecture_diagram": True,
                    "confidence": 60,
                    "description": "SVG file with diagram elements (needs manual review)",
                    "diagram_type": "svg_diagram",
                }
        except Exception:
            pass
        return {
            "is_architecture_diagram": False,
            "confidence": 30,
            "description": "SVG file - could not determine content",
            "diagram_type": "unknown",
        }

    # For raster images (.png, .jpg, .jpeg, .webp), use Databricks Model Serving
    image_data = base64.b64encode(file_bytes).decode("utf-8")

    # Determine MIME type
    mime_map = {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
    }
    mime_type = mime_map.get(ext, "image/png")

    prompt = """Analyze this image and determine if it is a SOFTWARE ARCHITECTURE DIAGRAM or SYSTEM DESIGN DIAGRAM related to machine learning, MLOps, AI platforms, data pipelines, or model serving infrastructure.

An architecture diagram typically shows:
- Components/services and their relationships
- Data flow between systems
- Deployment topology
- Pipeline stages
- Infrastructure layers

It is NOT an architecture diagram if it is:
- A screenshot of a UI/dashboard
- A logo, icon, or badge
- A photo of a person or physical object
- A chart/plot (bar chart, line chart, scatter plot, etc.)
- A code snippet image
- A meme or joke image
- A presentation slide with mostly text
- A simple flowchart for a tutorial step

Respond in JSON format ONLY:
{
    "is_architecture_diagram": true/false,
    "confidence": 0-100,
    "description": "brief description of what the image shows",
    "diagram_type": "system architecture / pipeline / deployment / data flow / component / none"
}"""

    try:
        response = requests.post(
            DATABRICKS_SERVING_URL,
            headers=_get_databricks_auth_headers(),
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": prompt},
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:{mime_type};base64,{image_data}",
                                },
                            },
                        ],
                    }
                ],
                "max_tokens": 300,
                "temperature": 0.1,
            },
            timeout=120,
        )

        if response.status_code == 429:
            # Rate limited by Databricks - wait and retry
            print("  Databricks rate limited. Waiting 10 seconds...")
            time.sleep(10)
            return classify_image_with_ai(file_bytes, file_path)

        if response.status_code != 200:
            print(f"  Databricks API error: {response.status_code} - {response.text[:200]}")
            return {
                "is_architecture_diagram": False,
                "confidence": 0,
                "description": f"API error: {response.status_code}",
                "diagram_type": "error",
            }

        result = response.json()
        content = result["choices"][0]["message"]["content"]

        # Parse JSON from response (handle markdown code blocks)
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1]
            content = content.rsplit("```", 1)[0]

        parsed = json.loads(content)
        return {
            "is_architecture_diagram": parsed.get("is_architecture_diagram", False),
            "confidence": parsed.get("confidence", 0),
            "description": parsed.get("description", ""),
            "diagram_type": parsed.get("diagram_type", "unknown"),
        }

    except json.JSONDecodeError as e:
        print(f"  Failed to parse AI response: {e}")
        return {
            "is_architecture_diagram": False,
            "confidence": 0,
            "description": "Failed to parse AI response",
            "diagram_type": "error",
        }
    except Exception as e:
        print(f"  AI classification error: {e}")
        return {
            "is_architecture_diagram": False,
            "confidence": 0,
            "description": f"Classification error: {e}",
            "diagram_type": "error",
        }


# ==========================================
# 8. Metadata and summary saving
# ==========================================

def save_metadata(rows):
    if not rows:
        return

    os.makedirs(BASE_SAVE_DIR, exist_ok=True)

    fieldnames = [
        "download_date",
        "query_source",
        "repo",
        "repo_url",
        "stars",
        "description",
        "file_path",
        "file_size",
        "heuristic_score",
        "ai_is_architecture",
        "ai_confidence",
        "ai_description",
        "ai_diagram_type",
        "github_url",
        "raw_url",
        "downloaded_path",
        "included_after_manual_check",
        "exclusion_reason",
    ]

    file_exists = os.path.exists(METADATA_CSV)

    with open(METADATA_CSV, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        for row in rows:
            writer.writerow(row)


def save_repo_summary(all_metadata_rows):
    """
    Generate a summary CSV showing per-repo statistics:
    - repo name, URL, description, stars
    - total diagrams found
    - AI-confirmed architecture diagrams count
    - list of confirmed diagram file paths
    """
    if not all_metadata_rows:
        return

    repo_data = {}

    for row in all_metadata_rows:
        repo = row["repo"]
        if repo not in repo_data:
            repo_data[repo] = {
                "repo": repo,
                "repo_url": row.get("repo_url", ""),
                "stars": row.get("stars", 0),
                "description": row.get("description", ""),
                "total_candidates": 0,
                "ai_confirmed_count": 0,
                "confirmed_diagrams": [],
            }

        repo_data[repo]["total_candidates"] += 1

        if row.get("ai_is_architecture") == "True":
            repo_data[repo]["ai_confirmed_count"] += 1
            repo_data[repo]["confirmed_diagrams"].append(row.get("file_path", ""))

    # Write summary CSV
    fieldnames = [
        "repo",
        "repo_url",
        "stars",
        "description",
        "total_candidates",
        "ai_confirmed_count",
        "confirmed_diagram_files",
    ]

    with open(SUMMARY_CSV, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for repo_name in sorted(repo_data.keys(), key=lambda r: repo_data[r]["ai_confirmed_count"], reverse=True):
            data = repo_data[repo_name]

            if data["ai_confirmed_count"] == 0:
                continue

            writer.writerow({
                "repo": data["repo"],
                "repo_url": data["repo_url"],
                "stars": data["stars"],
                "description": data["description"],
                "total_candidates": data["total_candidates"],
                "ai_confirmed_count": data["ai_confirmed_count"],
                "confirmed_diagram_files": " | ".join(data["confirmed_diagrams"]),
            })

    print(f"\nRepo summary saved to: {SUMMARY_CSV}")
    print(f"Repos with confirmed architecture diagrams: {sum(1 for d in repo_data.values() if d['ai_confirmed_count'] > 0)}")


def make_safe_filename(file_path):
    return (
        file_path
        .replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("*", "_")
        .replace("?", "_")
        .replace('"', "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace("|", "_")
    )


# ==========================================
# 9. Main scanning logic
# ==========================================

def main():
    os.makedirs(BASE_SAVE_DIR, exist_ok=True)

    seen_files = set()
    all_metadata_rows = []
    metadata_batch = []
    total_classified = 0
    total_ai_confirmed = 0

    repos_with_source = collect_repos()

    print("\n==========================================")
    print(f"Total unique repositories collected: {len(repos_with_source)}")
    print("Start scanning repository trees...")
    print("==========================================")

    for repo, query_source in repos_with_source:
        if total_ai_confirmed >= MAX_CANDIDATE_DIAGRAMS:
            break

        repo_name = repo.get("full_name")
        if not repo_name:
            continue

        if is_bad_repo(repo):
            print(f"Skip repo by blacklist: {repo_name}")
            continue

        default_branch = repo.get("default_branch") or "main"
        stars = repo.get("stargazers_count", 0)
        description = repo.get("description") or ""
        repo_url = repo.get("html_url") or f"https://github.com/{repo_name}"

        print(f"\nScanning repo: {repo_name} | stars: {stars} | source: {query_source}")

        tree = get_repo_tree(repo_name, default_branch)

        if not tree:
            print("  No tree returned.")
            continue

        candidates = []

        for item in tree:
            if item.get("type") != "blob":
                continue

            file_path = item.get("path", "")
            file_size = item.get("size", 0)

            if not file_path:
                continue

            if not is_target_file(file_path):
                continue

            if not file_path.lower().endswith(".drawio"):
                if file_size < MIN_FILE_SIZE or file_size > MAX_FILE_SIZE:
                    continue

            score = diagram_score(file_path)

            if score >= DIAGRAM_SCORE_THRESHOLD:
                candidates.append((file_path, score, file_size))

        candidates = sorted(candidates, key=lambda x: x[1], reverse=True)
        candidates = candidates[:MAX_DIAGRAMS_PER_REPO]

        if not candidates:
            print("  No candidate diagrams found.")
            time.sleep(1)
            continue

        safe_repo_folder = repo_name.replace("/", "_")
        repo_save_dir = os.path.join(BASE_SAVE_DIR, safe_repo_folder)

        for file_path, score, file_size in candidates:
            if total_ai_confirmed >= MAX_CANDIDATE_DIAGRAMS:
                break

            unique_key = f"{repo_name}/{file_path}"

            if unique_key in seen_files:
                continue

            seen_files.add(unique_key)

            raw_url = f"https://raw.githubusercontent.com/{repo_name}/{default_branch}/{file_path}"
            github_url = f"https://github.com/{repo_name}/blob/{default_branch}/{file_path}"

            # Fetch file into memory (not disk yet)
            file_bytes = fetch_file_bytes(raw_url)
            if not file_bytes:
                continue

            total_classified += 1

            # Run AI classification on in-memory bytes
            print(f"  Classifying [{total_classified}]: {file_path} (score={score})...")
            ai_result = classify_image_with_ai(file_bytes, file_path)

            is_arch = ai_result["is_architecture_diagram"]
            confidence = ai_result["confidence"]

            if is_arch and confidence >= AI_CONFIDENCE_THRESHOLD:
                total_ai_confirmed += 1
                status = "CONFIRMED"

                # Only save confirmed images to disk
                safe_file_name = make_safe_filename(file_path)
                save_path = os.path.join(repo_save_dir, safe_file_name)
                save_file(file_bytes, save_path)
            else:
                status = "REJECTED"
                save_path = ""  # Not saved to disk

            print(
                f"    -> {status} | confidence={confidence} | "
                f"type={ai_result['diagram_type']} | {ai_result['description'][:80]}"
            )

            row = {
                "download_date": datetime.now().strftime("%Y-%m-%d"),
                "query_source": query_source,
                "repo": repo_name,
                "repo_url": repo_url,
                "stars": stars,
                "description": description,
                "file_path": file_path,
                "file_size": file_size,
                "heuristic_score": score,
                "ai_is_architecture": str(is_arch and confidence >= AI_CONFIDENCE_THRESHOLD),
                "ai_confidence": confidence,
                "ai_description": ai_result["description"],
                "ai_diagram_type": ai_result["diagram_type"],
                "github_url": github_url,
                "raw_url": raw_url,
                "downloaded_path": save_path,
                "included_after_manual_check": "",
                "exclusion_reason": "",
            }

            metadata_batch.append(row)
            all_metadata_rows.append(row)

            # Small delay between AI calls to avoid rate limiting
            time.sleep(0.5)

        # Save metadata batch after each repo
        save_metadata(metadata_batch)
        metadata_batch = []

        time.sleep(1)

    # Save any remaining metadata
    save_metadata(metadata_batch)

    # Generate repo summary
    save_repo_summary(all_metadata_rows)

    # Final statistics
    print("\n==========================================")
    print("SCAN COMPLETE")
    print("==========================================")
    print(f"Total repositories scanned: {len(repos_with_source)}")
    print(f"Total candidates classified: {total_classified}")
    print(f"AI-confirmed architecture diagrams (saved): {total_ai_confirmed}")
    print(f"Confirmation rate: {total_ai_confirmed/max(total_classified,1)*100:.1f}%")
    print(f"")
    print(f"Files saved in: {BASE_SAVE_DIR}")
    print(f"Full metadata CSV: {METADATA_CSV}")
    print(f"Repo summary CSV: {SUMMARY_CSV}")
    print("==========================================")


if __name__ == "__main__":
    main()
