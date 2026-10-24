FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py .
# yt-dlp часто ломается после изменений на стороне TikTok — обновляем при каждом старте
CMD pip install -q -U "yt-dlp[default,curl-cffi]" && python bot.py
