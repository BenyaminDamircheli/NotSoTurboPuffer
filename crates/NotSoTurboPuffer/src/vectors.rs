use rkyv::{Archive, Deserialize, Serialize};

pub fn normalize_vector(vec: &[f32]) -> Vec<f32> {
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    // so we don't get NaN
    if norm <= 1e-6_f32 {
        // return an explicit zero vector instead of the tiny original vector
        return vec.iter().map(|_| 0.0_f32).collect();
    }
    vec.iter().map(|x| x / norm).collect()
}

#[derive(Archive, Serialize, Deserialize, Debug, Default)]
#[repr(u8)]
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DistanceMetric {
    #[default]
    CosineDistance,
    EuclideanSquared,
}

pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    1.0 - dot
}

pub fn cosine_distance_iter(a: impl Iterator<Item = f32>, b: &[f32]) -> f32 {
    let dot: f32 = a.zip(b).map(|(x, y)| x * y).sum();
    1.0 - dot
}

pub fn euclidean_squared(a: &[f32], b: &[f32]) -> f32 {
    a.iter()
        .zip(b)
        .map(|(x, y)| {
            let diff = x - y;
            diff * diff
        })
        .sum()
}

pub fn distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::CosineDistance => cosine_distance(a, b),
        DistanceMetric::EuclideanSquared => euclidean_squared(a, b),
    }
}
