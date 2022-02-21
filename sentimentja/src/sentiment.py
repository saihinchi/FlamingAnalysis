import tensorflow as tf


def preprocess(data, bpe, maxlen=310):
    return tf.keras.preprocessing.sequence.pad_sequences(
        [bpe(text) for text in data], maxlen=maxlen)


def predict(sentences, bpe, model, maxlen):
    preds = []
    targets = preprocess(sentences, bpe, maxlen=maxlen)
    return model.predict(targets)


def wrap(sentences, preds, emolabels):
    for i, ds in enumerate(preds):
        yield {"sentence": sentences[i], "emotions": dict(zip(emolabels, ds))}


def load(path):
    model = tf.keras.models.load_model(path)
    return model
