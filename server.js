require('dotenv').config();

const express = require('express');
const cors = require('cors');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require('@aws-sdk/s3-request-presigner');

const app = express();

app.use(cors({
  origin: '*',
  methods: ['GET', 'POST', 'HEAD', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

app.use(express.json({ limit: '1mb' }));

const PORT = process.env.PORT || 5050;
const REGION = process.env.AWS_REGION;
const BUCKET = process.env.S3_BUCKET;

if (!REGION || !BUCKET || !process.env.AWS_ACCESS_KEY_ID || !process.env.AWS_SECRET_ACCESS_KEY) {
  console.error('Missing required AWS environment variables');
  process.exit(1);
}

const s3 = new S3Client({
  region: REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});

const todayKst = () => {
  const now = new Date();
  const kst = new Date(now.getTime() + 9 * 60 * 60 * 1000);
  return kst.toISOString().slice(0, 10);
};

const isSafeId = (value) => {
  return typeof value === 'string' && /^[A-Za-z0-9_-]{3,80}$/.test(value);
};

app.get('/health', (req, res) => {
  res.json({
    success: true,
    service: 's3-presign-server',
    bucket: BUCKET,
    region: REGION,
  });
});

app.post('/api/v1/s3-presign', async (req, res) => {
  try {
    console.log('PRESIGN_REQUEST_RECEIVED', {
      time: new Date().toISOString(),
      body: req.body,
    });

    const incomingPrefix = req.body?.prefix;

let hunterId =
  req.body?.hunterId ||
  req.body?.hunter_id ||
  req.body?.metadata?.hunter_id ||
  req.body?.metadata?.hunter?.hunter_id;

let captureId =
  req.body?.captureId ||
  req.body?.capture_id;

let prefix;

if (incomingPrefix) {
  const match = incomingPrefix.match(
    /^v1\/raw\/([0-9]{4}-[0-9]{2}-[0-9]{2})\/([A-Za-z0-9_-]{3,80})\/([A-Za-z0-9_-]{3,120})\/$/
  );

  if (!match) {
    return res.status(400).json({
      success: false,
      message: 'Invalid S3 prefix format',
    });
  }

  const dateFromPrefix = match[1];
  hunterId = match[2];
  captureId = match[3];

  prefix = `v1/raw/${dateFromPrefix}/${hunterId}/${captureId}/`;
} else {
  if (!isSafeId(hunterId) || !isSafeId(captureId)) {
    return res.status(400).json({
      success: false,
      message: 'Invalid hunterId or captureId',
    });
  }

  const date = todayKst();
  prefix = `v1/raw/${date}/${hunterId}/${captureId}/`;
}

    const allowedFiles = {
      video: {
        fileName: 'video_raw.mp4',
        contentType: 'video/mp4',
      },
      captureMetadata: {
        fileName: 'capture_metadata_v1.json',
        contentType: 'application/json',
      },
      imuMetadata: {
        fileName: 'imu_metadata_v1.json',
        contentType: 'application/json',
      },
      uploadComplete: {
        fileName: 'upload_complete.json',
        contentType: 'application/json',
      },
    };

    const makeUrl = async ({ fileName, contentType }) => {
      const command = new PutObjectCommand({
        Bucket: BUCKET,
        Key: `${prefix}${fileName}`,
        ContentType: contentType,
      });

      return await getSignedUrl(s3, command, {
        expiresIn: 300,
      });
    };

    const urls = {
      video: await makeUrl(allowedFiles.video),
      captureMetadata: await makeUrl(allowedFiles.captureMetadata),
      imuMetadata: await makeUrl(allowedFiles.imuMetadata),
      uploadComplete: await makeUrl(allowedFiles.uploadComplete),
    };

    console.log('PRESIGN_URLS_CREATED', {
      time: new Date().toISOString(),
      prefix,
      hunterId,
      captureId,
    });

    return res.json({
      success: true,
      bucket: BUCKET,
      s3Prefix: prefix,
      urls,
    });
  } catch (error) {
    console.error('PRESIGN_ERROR:', error);

    return res.status(500).json({
      success: false,
      message: 'Failed to create presigned URLs',
    });
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`S3 Presign Server running on http://0.0.0.0:${PORT}`);
});