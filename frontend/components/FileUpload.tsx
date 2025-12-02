'use client';

import { useState, useRef } from 'react';

interface FileUploadProps {
  onFileSelect: (file: File) => void;
  accept?: string;
  label?: string;
}

export default function FileUpload({ onFileSelect, accept = '.csv,.xlsx', label = 'Upload Data' }: FileUploadProps) {
  const [dragActive, setDragActive] = useState(false);
  const [fileName, setFileName] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === 'dragenter' || e.type === 'dragover') {
      setDragActive(true);
    } else if (e.type === 'dragleave') {
      setDragActive(false);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);

    if (e.dataTransfer.files && e.dataTransfer.files[0]) {
      handleFile(e.dataTransfer.files[0]);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    e.preventDefault();
    if (e.target.files && e.target.files[0]) {
      handleFile(e.target.files[0]);
    }
  };

  const handleFile = (file: File) => {
    setFileName(file.name);
    onFileSelect(file);
  };

  const onButtonClick = () => {
    inputRef.current?.click();
  };

  return (
    <div className="w-full">
      <div
        className={`relative border-2 border-dashed rounded-xl p-6 text-center cursor-pointer transition-all duration-200 ${
          dragActive
            ? 'border-blue-500 bg-blue-500/10'
            : fileName
            ? 'border-green-500/50 bg-green-500/5'
            : 'border-slate-600/50 bg-slate-800/30 hover:border-slate-500/50 hover:bg-slate-700/30'
        }`}
        onDragEnter={handleDrag}
        onDragLeave={handleDrag}
        onDragOver={handleDrag}
        onDrop={handleDrop}
        onClick={onButtonClick}
      >
        <input
          ref={inputRef}
          type="file"
          className="hidden"
          accept={accept}
          onChange={handleChange}
        />

        {fileName ? (
          <>
            <div className="inline-flex items-center justify-center w-12 h-12 rounded-xl bg-green-500/20 mb-3">
              <svg className="w-6 h-6 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            </div>
            <p className="text-sm font-medium text-green-400 mb-1">{fileName}</p>
            <p className="text-xs text-slate-500">Click or drag to replace</p>
          </>
        ) : (
          <>
            <div className={`inline-flex items-center justify-center w-12 h-12 rounded-xl mb-3 transition-colors ${
              dragActive ? 'bg-blue-500/30' : 'bg-slate-700/50'
            }`}>
              <svg
                className={`w-6 h-6 transition-colors ${dragActive ? 'text-blue-400' : 'text-slate-400'}`}
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12"
                />
              </svg>
            </div>

            <p className={`text-sm mb-1 transition-colors ${dragActive ? 'text-blue-400' : 'text-slate-300'}`}>
              <span className="font-medium">Click to upload</span> or drag and drop
            </p>
            <p className="text-xs text-slate-500">CSV or Excel files</p>
          </>
        )}

        {/* Animated border on drag */}
        {dragActive && (
          <div className="absolute inset-0 rounded-xl border-2 border-blue-500 animate-pulse pointer-events-none"></div>
        )}
      </div>
    </div>
  );
}
