import torch
import torchvision.models as models
import torchvision.transforms as transforms
from PIL import Image
import argparse
import time

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


if __name__ == "__main__":
    start = time.time()
    resnet = models.resnet101(pretrained=True)
    resnet.eval()  # 设置模型为评估模式
    # 创建参数解析器
    parser = argparse.ArgumentParser(description='Image similarity comparison')
    parser.add_argument('origin', type=str, help='Path to the first image')
    parser.add_argument('target', type=str, help='Path to the second image')

    args = parser.parse_args()

    # 提取图像路径
    image_path1 = args.origin
    image_path2 = args.target
    # 提取图像特征
    features1 = extract_features(image_path1)
    features2 = extract_features(image_path2)

    # 计算相似度分数
    similarity_score = compute_similarity_score(features1, features2)
    end = time.time()
    print(similarity_score)
    print("{},{}".format(similarity_score,int((end-start)*1000)))
    # # 判断是否相似度超过0.9
    # if similarity_score > 0.9:
    #     print("true")
    # else:
    #     print("false")
