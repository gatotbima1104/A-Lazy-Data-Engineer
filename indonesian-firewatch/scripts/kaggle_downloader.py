import kaggle

# 1. Define the dataset identifier
# You can find this in the Kaggle dataset URL: ://kaggle.com
dataset_slug = "greegtitan/indonesia-province-city-district-and-subdistrict"

# 2. Specify where you want to save the files locally
output_path = "./data/kaggle"

print("Starting download...")

# 3. Download and automatically extract the files
kaggle.api.dataset_download_files(
    dataset_slug, 
    path=output_path, 
    unzip=True
)

print(f"Done! Dataset downloaded and unzipped to {output_path}")
