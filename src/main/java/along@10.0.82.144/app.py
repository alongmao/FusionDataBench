from flask import Flask, request
from textblob import TextBlob
import torch
import torchvision.models as models
import torchvision.transforms as transforms
from PIL import Image
import json

app = Flask(__name__)
resnet = models.resnet101(pretrained=True)
resnet.eval()  # 设置模型为评估模式

def analyze_sentiment(text):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity
    sentiment_type = 1
    if sentiment < 0:
        sentiment_type = 0
    elif sentiment > 0:
        sentiment_type = 2
    return sentiment_type,sentiment

def extract_features(image_path):
    # 加载图像
    image = Image.open(image_path).convert('RGB')

    # 定义预处理变换
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[
                             0.229, 0.224, 0.225])
    ])

    # 应用预处理变换
    input_tensor = preprocess(image)

    # 添加一个维度作为批处理维度
    input_batch = input_tensor.unsqueeze(0)

    # 将输入张量移动到所选设备上（如果可用）
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    input_batch = input_batch.to(device)

    # 提取图像特征
    with torch.no_grad():
        input_batch = input_batch.to(device)
        features = resnet(input_batch)

    # 将特征向量转换为一维张量
    features = torch.flatten(features, start_dim=1)

    # 返回特征向量
    return features


def compute_similarity_score(features1, features2):
    # 计算余弦相似度
    similarity_score = torch.cosine_similarity(features1, features2, dim=1)

    return similarity_score.item()

@app.route('/sentiment/predict', methods=['POST'])
def sentiment_predict():
    # 获取请求数据
    data = json.loads(request.get_data())
    text = data.get("text")
    sentiment_type, sentiment_score = analyze_sentiment(text)
    return {'sentiment_type': sentiment_type,'sentiment_score':sentiment_score}

@app.route('/image/similarity', methods=['GET'])
def image_similarity():
    origin = request.args.get('origin')
    target = request.args.get('target')
    
     # 提取图像特征
    features1 = extract_features(origin)
    features2 = extract_features(target)

    # 计算相似度分数
    similarity_score = compute_similarity_score(features1, features2)
    return {'similarity':similarity_score}

@app.route("/hello",methods=['GET'])
def say_hello():
    return "hello"

if __name__ == '__main__':
    app.run()
