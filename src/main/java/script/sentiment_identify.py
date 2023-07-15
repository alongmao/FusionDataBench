from snownlp import SnowNLP
import time
import argparse

def analyze_sentiment(text):
    s = SnowNLP(text)
    sentiment_score = s.sentiments
    if sentiment_score > 0.5:
        sentiment = "2"
    elif sentiment_score < 0.5:
        sentiment = "0"
    else:
        sentiment = "1"
    return sentiment,sentiment_score

if __name__ == "__main__":
    start = time.time()
    # 创建参数解析器
    parser = argparse.ArgumentParser(description='sentiment text identify')
    parser.add_argument('text', type=str, help='sentiment text')
    
    args = parser.parse_args()
    text = args.text

    # 进行情感分析
    sentiment_type, sentiment_score = analyze_sentiment(text)
    end = time.time()
    # 输出结果
    print("{},{}".format(sentiment_type,sentiment_score))
