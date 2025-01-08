import React, { useState, useCallback } from 'react';
import { useDropzone } from 'react-dropzone';
import { Upload } from '@aws-sdk/lib-storage';
import { S3Client } from '@aws-sdk/client-s3';
import { FileText, Database, Upload as UploadIcon, CheckCircle, AlertCircle, Server } from 'lucide-react';
import ETLPipelineVisualization from './ETLPipelineVisualization';

interface UploadStats {
  totalUploads: number;
  successfulUploads: number;
  failedUploads: number;
}

const Dashboard = () => {
  const [uploadProgress, setUploadProgress] = useState<{[key: string]: number}>({});
  const [uploadStatus, setUploadStatus] = useState<{[key: string]: string}>({});
  const [selectedFiles, setSelectedFiles] = useState<{[key: string]: File | null}>({
    xlsx: null,
    csv: null
  });
  const [stats, setStats] = useState<UploadStats>({
    totalUploads: 0,
    successfulUploads: 0,
    failedUploads: 0
  });

  // XLSX Dropzone
  const onDropXLSX = useCallback((acceptedFiles: File[]) => {
    const file = acceptedFiles[0];
    if (file) {
      setSelectedFiles(prev => ({ ...prev, xlsx: file }));
    }
  }, []);

  const { getRootProps: getXlsxRootProps, getInputProps: getXlsxInputProps, isDragActive: isXlsxDragActive } = useDropzone({
    onDrop: onDropXLSX,
    accept: { 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': ['.xlsx'] },
    multiple: false
  });

  // CSV Dropzone
  const onDropCSV = useCallback((acceptedFiles: File[]) => {
    const file = acceptedFiles[0];
    if (file) {
      setSelectedFiles(prev => ({ ...prev, csv: file }));
    }
  }, []);

  const { getRootProps: getCsvRootProps, getInputProps: getCsvInputProps, isDragActive: isCsvDragActive } = useDropzone({
    onDrop: onDropCSV,
    accept: { 'text/csv': ['.csv'] },
    multiple: false
  });

  const handleUpload = async (file: File, type: 'xlsx' | 'csv') => {
    if (!file) return;

    const fileId = `${type}-${Date.now()}`;
    setUploadProgress(prev => ({ ...prev, [fileId]: 0 }));
    setUploadStatus(prev => ({ ...prev, [fileId]: 'uploading' }));
    setStats(prev => ({ ...prev, totalUploads: prev.totalUploads + 1 }));

    try {
      const s3Client = new S3Client({
        region: import.meta.env.VITE_AWS_REGION,
        credentials: {
          accessKeyId: import.meta.env.VITE_AWS_ACCESS_KEY_ID,
          secretAccessKey: import.meta.env.VITE_AWS_SECRET_ACCESS_KEY,
        },
      });

      const folder = type === 'xlsx' ? 'raw' : 'processed';
      
      const upload = new Upload({
        client: s3Client,
        params: {
          Bucket: import.meta.env.VITE_S3_BUCKET,
          Key: `${folder}/${file.name}`,
          Body: file,
          ContentType: file.type,
        },
        queueSize: 1,
        partSize: 1024 * 1024 * 5,
        leavePartsOnError: false,
      });

      upload.on('httpUploadProgress', (progress) => {
        const percentage = Math.round((progress.loaded || 0) * 100 / (progress.total || 100));
        setUploadProgress(prev => ({ ...prev, [fileId]: percentage }));
      });

      await upload.done();
      setUploadStatus(prev => ({ ...prev, [fileId]: 'complete' }));
      setSelectedFiles(prev => ({ ...prev, [type]: null }));
      setStats(prev => ({ ...prev, successfulUploads: prev.successfulUploads + 1 }));

      setTimeout(() => {
        setUploadProgress(prev => {
          const newProgress = { ...prev };
          delete newProgress[fileId];
          return newProgress;
        });
        setUploadStatus(prev => {
          const newStatus = { ...prev };
          delete newStatus[fileId];
          return newStatus;
        });
      }, 3000);
    } catch (error) {
      console.error('Upload error:', error);
      setUploadStatus(prev => ({ ...prev, [fileId]: 'failed' }));
      setStats(prev => ({ ...prev, failedUploads: prev.failedUploads + 1 }));
    }
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 py-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between">
            <div className="flex items-center">
              <Server className="h-8 w-8 text-blue-500 mr-3" />
              <h1 className="text-2xl font-bold text-gray-900">Data Bridge ETL Dashboard</h1>
            </div>
            <div className="flex items-center space-x-4">
              <div className="text-sm text-gray-600">
                Region: {import.meta.env.VITE_AWS_REGION}
              </div>
              <div className="text-sm text-gray-600">
                Bucket: {import.meta.env.VITE_S3_BUCKET}
              </div>
            </div>
          </div>
        </div>
      </header>

      <main className="max-w-7xl mx-auto px-4 py-6 sm:px-6 lg:px-8">
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center">
              <div className="bg-blue-100 rounded-full p-3">
                <UploadIcon className="h-6 w-6 text-blue-600" />
              </div>
              <div className="ml-4">
                <h2 className="text-sm font-medium text-gray-500">Total Uploads</h2>
                <p className="text-2xl font-semibold text-gray-900">{stats.totalUploads}</p>
              </div>
            </div>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center">
              <div className="bg-green-100 rounded-full p-3">
                <CheckCircle className="h-6 w-6 text-green-600" />
              </div>
              <div className="ml-4">
                <h2 className="text-sm font-medium text-gray-500">Successful</h2>
                <p className="text-2xl font-semibold text-gray-900">{stats.successfulUploads}</p>
              </div>
            </div>
          </div>
          <div className="bg-white rounded-lg shadow p-6">
            <div className="flex items-center">
              <div className="bg-red-100 rounded-full p-3">
                <AlertCircle className="h-6 w-6 text-red-600" />
              </div>
              <div className="ml-4">
                <h2 className="text-sm font-medium text-gray-500">Failed</h2>
                <p className="text-2xl font-semibold text-gray-900">{stats.failedUploads}</p>
              </div>
            </div>
          </div>
        </div>

        <ETLPipelineVisualization 
          isProcessing={Object.keys(uploadProgress).length > 0}
          fileType={Object.keys(selectedFiles).find(key => selectedFiles[key]) as 'xlsx' | 'csv' || 'xlsx'}
        />

        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mt-8">
          <div className="bg-white rounded-lg shadow">
            <div className="p-6">
              <div className="flex items-center mb-4">
                <Database className="h-6 w-6 text-blue-500 mr-2" />
                <h2 className="text-lg font-semibold text-gray-900">XLSX Upload</h2>
                <span className="ml-2 text-sm text-gray-500">(to /raw)</span>
              </div>
              <div
                {...getXlsxRootProps()}
                className={`p-6 border-2 border-dashed rounded-lg text-center cursor-pointer transition-all duration-200 
                  ${isXlsxDragActive ? 'border-blue-500 bg-blue-50' : 'border-gray-300 hover:border-gray-400'}`}
              >
                <input {...getXlsxInputProps()} />
                <div className="space-y-2">
                  <Database className="h-12 w-12 mx-auto text-gray-400" />
                  <p className="text-gray-600">
                    {isXlsxDragActive
                      ? "Drop your XLSX file here..."
                      : "Drag & drop XLSX file, or click to select"}
                  </p>
                </div>
              </div>
            </div>
            {selectedFiles.xlsx && (
              <div className="px-6 pb-6">
                <div className="mt-4 p-4 bg-gray-50 rounded-lg">
                  <p className="text-sm text-gray-700">Selected: {selectedFiles.xlsx.name}</p>
                  <button
                    onClick={() => handleUpload(selectedFiles.xlsx!, 'xlsx')}
                    className="mt-3 w-full py-2 px-4 rounded bg-blue-500 text-white font-medium hover:bg-blue-600 
                      transition-colors duration-200 disabled:bg-gray-400"
                    disabled={!selectedFiles.xlsx}
                  >
                    Upload to Raw
                  </button>
                </div>
              </div>
            )}
          </div>

          <div className="bg-white rounded-lg shadow">
            <div className="p-6">
              <div className="flex items-center mb-4">
                <FileText className="h-6 w-6 text-green-500 mr-2" />
                <h2 className="text-lg font-semibold text-gray-900">CSV Upload</h2>
                <span className="ml-2 text-sm text-gray-500">(to /processed)</span>
              </div>
              <div
                {...getCsvRootProps()}
                className={`p-6 border-2 border-dashed rounded-lg text-center cursor-pointer transition-all duration-200 
                  ${isCsvDragActive ? 'border-green-500 bg-green-50' : 'border-gray-300 hover:border-gray-400'}`}
              >
                <input {...getCsvInputProps()} />
                <div className="space-y-2">
                  <FileText className="h-12 w-12 mx-auto text-gray-400" />
                  <p className="text-gray-600">
                    {isCsvDragActive
                      ? "Drop your CSV file here..."
                      : "Drag & drop CSV file, or click to select"}
                  </p>
                </div>
              </div>
            </div>
            {selectedFiles.csv && (
              <div className="px-6 pb-6">
                <div className="mt-4 p-4 bg-gray-50 rounded-lg">
                  <p className="text-sm text-gray-700">Selected: {selectedFiles.csv.name}</p>
                  <button
                    onClick={() => handleUpload(selectedFiles.csv!, 'csv')}
                    className="mt-3 w-full py-2 px-4 rounded bg-green-500 text-white font-medium hover:bg-green-600 
                      transition-colors duration-200 disabled:bg-gray-400"
                    disabled={!selectedFiles.csv}
                  >
                    Upload to Processed
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>

        {Object.entries(uploadProgress).map(([id, progress]) => (
          <div key={id} className="mt-6 bg-white rounded-lg shadow p-4">
            <div className="flex items-center justify-between mb-2">
              <span className="text-sm font-medium text-gray-700">
                {id.split('-')[0].toUpperCase()} Upload Progress
              </span>
              <span className="text-sm text-gray-500">{progress}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2">
              <div
                className={`h-2 rounded-full transition-all duration-300 ${
                  uploadStatus[id] === 'failed' ? 'bg-red-500' : 'bg-blue-500'
                }`}
                style={{ width: `${progress}%` }}
              ></div>
            </div>
            {uploadStatus[id] && (
              <p className={`mt-2 text-sm ${
                uploadStatus[id] === 'failed' ? 'text-red-500' : 
                uploadStatus[id] === 'complete' ? 'text-green-500' : 
                'text-gray-600'
              }`}>
                Status: {uploadStatus[id].charAt(0).toUpperCase() + uploadStatus[id].slice(1)}
              </p>
            )}
          </div>
        ))}
      </main>
    </div>
  );
};

export default Dashboard;