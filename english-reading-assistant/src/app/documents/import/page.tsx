import { AppShell } from "@/components/app-shell";
import { ExtensionTokenPanel } from "@/components/extension-token-panel";
import { PageHeader } from "@/components/page-header";
import { UploadForm } from "@/components/upload-form";
import { WebImportForm } from "@/components/web-import-form";
import { requireUser } from "@/lib/auth";

export default async function ImportPage() {
  const { profile } = await requireUser();

  return (
    <AppShell profile={profile}>
      <PageHeader
        title="导入材料"
        description="上传 PDF/DOCX，或粘贴网页正文。安装插件后可以直接从当前网页保存到文章库。"
      />
      <div className="grid gap-5 lg:grid-cols-2">
        <UploadForm />
        <WebImportForm />
      </div>
      <div className="mt-5">
        <ExtensionTokenPanel />
      </div>
    </AppShell>
  );
}
