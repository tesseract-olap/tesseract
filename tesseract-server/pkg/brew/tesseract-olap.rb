class TesseractOlap < Formula
  version '0.13.0'
  desc "ROLAP engine for web applications, in Rust."
  homepage "https://github.com/hwchen/tesseract"
  url "https://github.com/hwchen/tesseract/releases/download/v#{version}/tesseract-olap-#{version}-x86_64-apple-darwin.tar.gz"
  sha256 "5b30018ee4a0a31c78bcbf8524ba153ac16ded5ce4b20f5483f9a5c4f2e40c97"

  def install
    bin.install "tesseract-olap"
  end
end

