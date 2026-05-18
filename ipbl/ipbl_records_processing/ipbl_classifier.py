from transformers import AutoTokenizer, AutoModelForSequenceClassification
from huggingface_hub import hf_hub_download
import joblib, torch


class IPBLClassifier:

    def __init__(self):
        self.model_id = "darekpe79/true-false-pbl-herbert"
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_id, use_safetensors=True)
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_id)

        self.enc_path  = hf_hub_download(repo_id=self.model_id, filename="label_encoder.joblib")
        self.label_enc = joblib.load(self.enc_path)

    def predict_pbl(self, input_txt):
        inputs = self.tokenizer(input_txt, return_tensors="pt", truncation=True, padding=True)
        self.model.eval()
        with torch.no_grad():
            pred_id = self.model(**inputs).logits.argmax(-1).item()
        return eval(self.label_enc.inverse_transform([pred_id])[0]) 