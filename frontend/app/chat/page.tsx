'use client';

import { useState } from 'react';
import Navigation from '@/components/Navigation';
import ChatInterface from '@/components/ChatInterface';
import FileUpload from '@/components/FileUpload';
import { ChatMessage } from '@/types';
import { api } from '@/lib/api';

export default function ChatPage() {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [sessionId, setSessionId] = useState<string | undefined>();
  const [isLoading, setIsLoading] = useState(false);
  const [hasData, setHasData] = useState(false);
  const [fileName, setFileName] = useState<string>('');

  const handleFileSelect = async (file: File) => {
    setIsLoading(true);
    setFileName(file.name);
    try {
      const result = await api.chatWithData('Analyze this data and give me a summary.', file) as any;
      setSessionId(result.session_id);
      setMessages([
        { role: 'assistant', content: result.response || result.message, timestamp: new Date() }
      ]);
      setHasData(true);
    } catch (error) {
      console.error('Failed to load data:', error);
      setMessages([
        { role: 'assistant', content: 'Failed to load data. Please make sure the backend is running and try again.', timestamp: new Date() }
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSendMessage = async (message: string) => {
    const userMessage: ChatMessage = {
      role: 'user',
      content: message,
      timestamp: new Date()
    };
    setMessages(prev => [...prev, userMessage]);
    setIsLoading(true);

    try {
      const result = await api.chat(message, sessionId) as any;
      setSessionId(result.session_id);

      const assistantMessage: ChatMessage = {
        role: 'assistant',
        content: result.response || result.message,
        timestamp: new Date()
      };
      setMessages(prev => [...prev, assistantMessage]);
    } catch (error) {
      console.error('Failed to send message:', error);
      const errorMessage: ChatMessage = {
        role: 'assistant',
        content: 'Sorry, I encountered an error. Please check that the backend is running and try again.',
        timestamp: new Date()
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleClearChat = () => {
    setMessages([]);
    setSessionId(undefined);
    setHasData(false);
    setFileName('');
  };

  return (
    <div className="min-h-screen bg-[#0f172a]">
      <Navigation />

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="mb-8 animate-fade-in">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-white mb-2">
                Chat with <span className="gradient-text">Echo</span>
              </h1>
              <p className="text-slate-400">
                Upload your data and ask questions in natural language
              </p>
            </div>
            {hasData && (
              <button
                onClick={handleClearChat}
                className="px-4 py-2 text-sm text-slate-400 hover:text-white bg-slate-800/50 hover:bg-slate-700/50 rounded-lg border border-slate-700/50 transition-all"
              >
                New Chat
              </button>
            )}
          </div>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-4 gap-8">
          {/* Sidebar */}
          <div className="lg:col-span-1 space-y-6">
            {/* Data Upload Card */}
            <div className="bg-slate-800/50 rounded-xl p-5 border border-slate-700/50 animate-fade-in">
              <div className="flex items-center space-x-2 mb-4">
                <div className="w-8 h-8 rounded-lg bg-blue-500/20 flex items-center justify-center">
                  <svg className="w-4 h-4 text-blue-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                  </svg>
                </div>
                <h3 className="text-white font-medium">Data Source</h3>
              </div>

              {hasData ? (
                <div className="space-y-3">
                  <div className="flex items-center space-x-3 p-3 bg-green-500/10 rounded-lg border border-green-500/20">
                    <div className="w-8 h-8 rounded-lg bg-green-500/20 flex items-center justify-center">
                      <svg className="w-4 h-4 text-green-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                      </svg>
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm text-green-400 font-medium truncate">{fileName}</p>
                      <p className="text-xs text-green-400/70">Data loaded</p>
                    </div>
                  </div>
                  <p className="text-xs text-slate-500">
                    You can now ask questions about your data.
                  </p>
                </div>
              ) : (
                <div className="space-y-3">
                  <FileUpload onFileSelect={handleFileSelect} />
                  <p className="text-xs text-slate-500 text-center">
                    Upload a CSV or Excel file to get started
                  </p>
                </div>
              )}
            </div>

            {/* Tips Card */}
            <div className="bg-slate-800/50 rounded-xl p-5 border border-slate-700/50 animate-fade-in" style={{ animationDelay: '100ms' }}>
              <div className="flex items-center space-x-2 mb-4">
                <div className="w-8 h-8 rounded-lg bg-purple-500/20 flex items-center justify-center">
                  <svg className="w-4 h-4 text-purple-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                  </svg>
                </div>
                <h3 className="text-white font-medium">Tips</h3>
              </div>
              <ul className="space-y-2 text-xs text-slate-400">
                <li className="flex items-start space-x-2">
                  <span className="text-blue-400 mt-0.5">•</span>
                  <span>Ask about revenue trends, growth rates, or comparisons</span>
                </li>
                <li className="flex items-start space-x-2">
                  <span className="text-blue-400 mt-0.5">•</span>
                  <span>Request specific metrics like MRR, CAC, or LTV</span>
                </li>
                <li className="flex items-start space-x-2">
                  <span className="text-blue-400 mt-0.5">•</span>
                  <span>Ask for recommendations based on your data</span>
                </li>
              </ul>
            </div>
          </div>

          {/* Chat Area */}
          <div className="lg:col-span-3 animate-fade-in" style={{ animationDelay: '50ms' }}>
            <ChatInterface
              onSendMessage={handleSendMessage}
              messages={messages}
              isLoading={isLoading}
            />
          </div>
        </div>
      </main>
    </div>
  );
}
