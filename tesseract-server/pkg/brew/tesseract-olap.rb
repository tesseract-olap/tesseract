class TesseractOlap < Formula
  version '0.12.0'
  desc "ROLAP engine for web applications, in Rust."
  homepage "https://github.com/hwchen/tesseract"
  url "https://github.com/hwchen/tesseract/releases/download/v#{version}/tesseract-olap-#{version}-x86_64-apple-darwin.tar.gz"
  sha256 "0e13cfb7505fc5b786039cfe9007842ba0682f07217beef1e94d86de8c78c740"

  def install
    bin.install "release/tesseract-olap"
  end
end

