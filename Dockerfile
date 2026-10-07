FROM python:3.12-slim

WORKDIR /app
COPY . .

EXPOSE 8000

# 依赖随容器启动安装，然后启动服务
CMD ["sh", "-c", "pip install --no-cache-dir -r requirements.txt && python app.py"]
