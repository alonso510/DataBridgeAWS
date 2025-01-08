import React, { useState, useEffect } from 'react';
import { Database, FileText, Server, ArrowRight, CheckCircle, Loader2 } from 'lucide-react';

interface ETLStage {
  name: string;
  status: 'idle' | 'processing' | 'complete' | 'error';
  progress: number;
  processingTime?: number;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
const ETLPipelineVisualization = ({ isProcessing, fileType }: { isProcessing: boolean; fileType: 'xlsx' | 'csv' }) => {
  const [stages, setStages] = useState<ETLStage[]>([
    { name: 'Upload', status: 'idle', progress: 0 },
    { name: 'Transform', status: 'idle', progress: 0 },
    { name: 'Load', status: 'idle', progress: 0 }
  ]);

  const randomTime = () => Math.floor(Math.random() * (60 - 45 + 1) + 45);
  const randomProgress = () => Math.floor(Math.random() * 30) + 70; // 70-100% completion

  useEffect(() => {
    if (isProcessing) {
      // Reset stages
      setStages(prev => prev.map(stage => ({ ...stage, status: 'idle', progress: 0 })));

      // Start Upload Stage
      setStages(prev => prev.map((stage, i) => 
        i === 0 ? { ...stage, status: 'processing', progress: 0 } : stage
      ));

      // Simulate upload completion
      const uploadTime = randomTime() / 3;
      setTimeout(() => {
        setStages(prev => prev.map((stage, i) => 
          i === 0 ? { ...stage, status: 'complete', progress: 100 } : 
          i === 1 ? { ...stage, status: 'processing', progress: 0 } : stage
        ));

        // Simulate transform stage
        const transformTime = randomTime() / 3;
        setTimeout(() => {
          setStages(prev => prev.map((stage, i) => 
            i === 1 ? { ...stage, status: 'complete', progress: 100 } : 
            i === 2 ? { ...stage, status: 'processing', progress: 0 } : stage
          ));

          // Simulate load stage
          const loadTime = randomTime() / 3;
          setTimeout(() => {
            setStages(prev => prev.map(stage => 
              ({ ...stage, status: 'complete', progress: 100 })
            ));
          }, loadTime * 1000);
        }, transformTime * 1000);
      }, uploadTime * 1000);
    }
  }, [isProcessing]);

  // Progress animation
  useEffect(() => {
    const interval = setInterval(() => {
      setStages(prev => prev.map(stage => 
        stage.status === 'processing' && stage.progress < randomProgress()
          ? { ...stage, progress: stage.progress + 1 }
          : stage
      ));
    }, 100);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="bg-white rounded-lg shadow p-6 mt-8">
      <h3 className="text-lg font-semibold text-gray-900 mb-6">ETL Pipeline Status</h3>
      <div className="flex items-center justify-between max-w-3xl mx-auto">
        {stages.map((stage, index) => (
          <React.Fragment key={stage.name}>
            <div className="flex flex-col items-center">
              <div className={`w-16 h-16 rounded-full flex items-center justify-center 
                ${stage.status === 'processing' ? 'bg-blue-100' :
                  stage.status === 'complete' ? 'bg-green-100' :
                  'bg-gray-100'}`}>
                {stage.name === 'Upload' && <Database className={`h-8 w-8 
                  ${stage.status === 'processing' ? 'text-blue-500' :
                    stage.status === 'complete' ? 'text-green-500' :
                    'text-gray-500'}`} />}
                {stage.name === 'Transform' && <FileText className={`h-8 w-8 
                  ${stage.status === 'processing' ? 'text-blue-500' :
                    stage.status === 'complete' ? 'text-green-500' :
                    'text-gray-500'}`} />}
                {stage.name === 'Load' && <Server className={`h-8 w-8 
                  ${stage.status === 'processing' ? 'text-blue-500' :
                    stage.status === 'complete' ? 'text-green-500' :
                    'text-gray-500'}`} />}
              </div>
              <div className="mt-2 text-sm font-medium text-gray-900">{stage.name}</div>
              <div className="mt-1 text-xs text-gray-500">
                {stage.status === 'processing' ? `${stage.progress}%` :
                 stage.status === 'complete' ? 'Complete' :
                 'Waiting...'}
              </div>
              {stage.status === 'processing' && (
                <Loader2 className="mt-2 h-4 w-4 text-blue-500 animate-spin" />
              )}
              {stage.status === 'complete' && (
                <CheckCircle className="mt-2 h-4 w-4 text-green-500" />
              )}
            </div>
            {index < stages.length - 1 && (
              <div className="w-24 h-px bg-gray-300 relative">
                <ArrowRight className={`absolute top-1/2 left-1/2 transform -translate-y-1/2 -translate-x-1/2 h-5 w-5
                  ${stages[index].status === 'complete' ? 'text-green-500' : 'text-gray-300'}`} />
              </div>
            )}
          </React.Fragment>
        ))}
      </div>
    </div>
  );
};

export default ETLPipelineVisualization;