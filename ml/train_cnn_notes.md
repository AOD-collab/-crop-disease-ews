# Training a real CNN on leaf photos — moved

This used to be a set of notes. It's now a real, separate, runnable
project: **`maize_model_training/`** (sibling folder to this whole app).

See `maize_model_training/README.md` for the full workflow (prepare
dataset -> train -> evaluate -> export), and `ml/trained_model/README.md`
in this project for how to plug the exported model in here once it's ready.

Quick version:

```bash
cd ../maize_model_training
pip install -r requirements.txt
# add labeled photos under data/train/<class>/ and data/val/<class>/
python train.py
python export_model.py
cp exported/saved_model_cnn.pt ../crop_disease_image_system/ml/trained_model/
```
