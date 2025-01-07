import React, { useCallback, useState } from 'react';
import { useDropzone } from 'react-dropzone';
import { Upload } from '@aws-sdk/lib-storage';
import { S3Client } from '@aws-sdk/client-s3';

const FileUpload = () => {
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadStatus, setUploadStatus] = useState('');
  const [selectedFile, setSelectedFile] = useState<File | null>(null);

  const handleUpload = async (file: File) => {
    if (!file) {
      setUploadStatus('Please select a file first');
      return;
    }

    if (!file.name.endsWith('.xlsx')) {
      setUploadStatus('Please upload only XLSX files');
      return;
    }

    try {
      const s3Client = new S3Client({
        region: import.meta.env.VITE_AWS_REGION,
        credentials: {
          accessKeyId: import.meta.env.VITE_AWS_ACCESS_KEY_ID,
          secretAccessKey: import.meta.env.VITE_AWS_SECRET_ACCESS_KEY,
        },
      });

      const upload = new Upload({
        client: s3Client,
        params: {
          Bucket: import.meta.env.VITE_S3_BUCKET,
          Key: `raw/${file.name}`,
          Body: file,
          ContentType: file.type,
        },
        queueSize: 1,
        partSize: 1024 * 1024 * 5, // 5MB
        leavePartsOnError: false,
      });

      upload.on('httpUploadProgress', (progress) => {
        const percentage = Math.round((progress.loaded || 0) * 100 / (progress.total || 100));
        setUploadProgress(percentage);
      });

      await upload.done();
      setUploadStatus('Upload complete!');
      setSelectedFile(null);
      setTimeout(() => {
        setUploadProgress(0);
        setUploadStatus('');
      }, 3000);
    } catch (error) {
      setUploadStatus('Upload failed. Please try again.');
      console.error('Upload error:', error);
    }
  };

  const onDrop = useCallback((acceptedFiles: File[]) => {
    const file = acceptedFiles[0];
    if (file) {
      setSelectedFile(file);
    }
  }, []);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({
    onDrop,
    accept: {
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx']
    },
    multiple: false
  });

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="w-full max-w-xl p-8">
        <div
          {...getRootProps()}
          className={`p-8 border-2 border-dashed rounded-lg text-center cursor-pointer transition-all duration-200 
            ${isDragActive ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:border-gray-400'}`}
        >
          <input {...getInputProps()} />
          <div className="space-y-4">
            <div className="text-4xl mb-4">📊</div>
            <p className="text-lg text-gray-600">
              {isDragActive
                ? "Drop your XLSX file here..."
                : "Drag & drop an XLSX file here, or click to select"}
            </p>
            <p className="text-sm text-gray-500">Only XLSX files are accepted</p>
          </div>
        </div>

        {selectedFile && (
          <div className="mt-4 p-4 bg-white rounded-lg shadow-sm">
            <p className="text-gray-700">Selected file: {selectedFile.name}</p>
          </div>
        )}

        <button
          onClick={() => selectedFile && handleUpload(selectedFile)}
          disabled={!selectedFile || uploadProgress > 0}
          className={`mt-4 w-full py-3 px-4 rounded-lg text-white font-medium transition-all duration-200
            ${!selectedFile || uploadProgress > 0 
              ? 'bg-gray-400 cursor-not-allowed' 
              : 'bg-blue-500 hover:bg-blue-600'}`}
        >
          {uploadProgress > 0 ? `Uploading... ${uploadProgress}%` : 'Upload File'}
        </button>

        {uploadStatus && (
          <p className={`mt-4 text-center ${
            uploadStatus.includes('failed') ? 'text-red-500' : 
            uploadStatus.includes('complete') ? 'text-green-500' : 
            'text-gray-600'
          }`}>
            {uploadStatus}
          </p>
        )}

        {uploadProgress > 0 && (
          <div className="mt-4 w-full bg-gray-200 rounded-full h-2.5">
            <div 
              className="bg-blue-600 h-2.5 rounded-full transition-all duration-300"
              style={{ width: `${uploadProgress}%` }}
            ></div>
          </div>
        )}
      </div>
    </div>
  );
};

export default FileUpload;