FROM python:3.9
WORKDIR /app
COPY . .


RUN python3 -m pip install --break-system-packages --no-cache-dir -r requirements.txt

EXPOSE 5000
CMD ["python3", "lb.py"]