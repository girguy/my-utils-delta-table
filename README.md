# my-utils-delta-table

A lightweight utility package to simplify working with **Delta Lake tables** on **MinIO/S3**, using **Polars**, **boto3**, and **deltalake**.

---

## ✨ Features

- Connect securely to S3/MinIO with configurable endpoints
- Automatically create buckets if they don't exist
- Generate test datasets with random strings
- Write and read Delta tables using PyArrow and Delta Lake
- Partition tables by `year`, `month`, and `day`
- Logging support for better debugging

---

## 📦 Installation

First, install the package in **editable mode** using `uv`:

```bash
uv pip install -e /path/to/my-utils-delta-table
